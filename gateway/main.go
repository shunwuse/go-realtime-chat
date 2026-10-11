package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// Heartbeat: ping every 30s, read must succeed within 60s, writes within 10s.
	pingInterval = 30 * time.Second
	pongTimeout  = 60 * time.Second
	writeTimeout = 10 * time.Second
	// rpcTimeout bounds every unary call so a stuck chatservice never hangs a browser.
	rpcTimeout = 2 * time.Second
)

func main() {
	// Right hand: gRPC client to chatservice. Locally :50051, in compose the service name.
	addr := os.Getenv("CHAT_ADDR")
	if addr == "" {
		addr = "localhost:50051"
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial chatservice %s: %v", addr, err)
	}

	chat := pb.NewChatServiceClient(conn)

	hub := NewHub()
	go hub.Run()

	http.HandleFunc("/", servePage)
	http.HandleFunc("/ws", func(w http.ResponseWriter, request *http.Request) {
		serveWS(hub, chat, w, request)
	})

	log.Println("gateway listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// wsIncoming is what browsers send: plain JSON with the message body.
// Room and sender come from the query string, so they are not repeated here.
type wsIncoming struct {
	Body string `json:"body"`
}

// wsOutgoing is what browsers receive: one chat event encoded as JSON.
type wsOutgoing struct {
	EventID      int64  `json:"event_id"`
	RoomID       string `json:"room_id"`
	From         string `json:"from"`
	Body         string `json:"body"`
	SentAtUnixMs int64  `json:"sent_at_unix_ms"`
}

// wsError is a private reply to the sender only, never broadcast.
type wsError struct {
	Error string `json:"error"`
}

// serveWS upgrades one browser connection and joins it to a room.
// Query: /ws?room=<room>&from=<name>[&last_event_id=<n>].
// With last_event_id, missed events are pushed before joining for live traffic.
func serveWS(hub *Hub, chat pb.ChatServiceClient, w http.ResponseWriter, r *http.Request) {
	room := r.URL.Query().Get("room")
	from := r.URL.Query().Get("from")
	if room == "" || from == "" {
		http.Error(w, "room and from are required", http.StatusBadRequest)

		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}

	c := &Client{
		hub:  hub,
		room: room,
		from: from,
		send: make(chan []byte, 256),
	}

	// Catch-up: fill the gap before joining, so the client never misses history.
	if sinceRaw := r.URL.Query().Get("last_event_id"); sinceRaw != "" {
		since, err := strconv.ParseInt(sinceRaw, 10, 64)
		if err != nil {
			http.Error(w, "last_event_id must be an integer", http.StatusBadRequest)

			return
		}

		history, err := fetchHistory(r.Context(), chat, room, since)
		if err != nil {
			http.Error(w, "chatservice unavailable", http.StatusBadGateway)

			return
		}

		// The send queue holds 256: keep only the newest on overflow.
		if len(history) > 256 {
			history = history[len(history)-256:]
		}

		for _, data := range history {
			select {
			case c.send <- data:
			default:
			}
		}
	}

	hub.register <- c

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go writePump(ctx, conn, c)
	readPump(ctx, conn, hub, chat, c)
}

// fetchHistory returns missed events encoded as wsOutgoing JSON, oldest first.
func fetchHistory(ctx context.Context, chat pb.ChatServiceClient, room string, since int64) ([][]byte, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	history, err := chat.GetHistory(rpcCtx, &pb.GetHistoryRequest{
		RoomId:       room,
		SinceEventId: since,
		Limit:        200,
	})
	if err != nil {
		return nil, err
	}

	var missed [][]byte
	for _, event := range history.GetEvents() {
		data, err := json.Marshal(encodeEvent(event))
		if err != nil {
			return nil, err
		}

		missed = append(missed, data)
	}

	return missed, nil
}

// encodeEvent converts one ChatEvent to its browser JSON form.
func encodeEvent(event *pb.ChatEvent) wsOutgoing {
	return wsOutgoing{
		EventID:      event.GetEventId(),
		RoomID:       event.GetRoomId(),
		From:         event.GetFrom(),
		Body:         event.GetBody(),
		SentAtUnixMs: event.GetSentAtUnixMs(),
	}
}

// replyError sends a private error to the sender only, never broadcasting it.
func replyError(c *Client, message string) {
	data, err := json.Marshal(wsError{Error: message})
	if err != nil {
		return
	}

	select {
	case c.send <- data:
	default:
	}
}

// readPump forwards browser text messages to chatservice, then broadcasts results.
// It returns when the browser goes away, then unregisters the client.
func readPump(ctx context.Context, conn *websocket.Conn, hub *Hub, chat pb.ChatServiceClient, c *Client) {
	defer func() {
		hub.unregister <- c
		conn.Close(websocket.StatusNormalClosure, "")
	}()

	conn.SetReadLimit(1 << 20) // 1MB max per message

	for {
		// Pong timeout: no frame within 60s means a dead connection.
		rctx, cancel := context.WithTimeout(ctx, pongTimeout)
		_, data, err := conn.Read(rctx)
		cancel()

		if err != nil {
			return
		}

		handleMessage(ctx, hub, chat, c, data)
	}
}

// handleMessage stores one browser message via Send, then broadcasts the stored event.
// Failures go back to the sender only; the room never sees them.
func handleMessage(ctx context.Context, hub *Hub, chat pb.ChatServiceClient, c *Client, data []byte) {
	var incoming wsIncoming
	if err := json.Unmarshal(data, &incoming); err != nil {
		replyError(c, "message must be JSON like {\"body\":\"hi\"}")

		return
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	response, err := chat.Send(rpcCtx, &pb.SendRequest{
		RoomId: c.room,
		From:   c.from,
		Body:   incoming.Body,
	})
	cancel()

	if err != nil {
		replyError(c, err.Error())

		return
	}

	// The stored timestamp lives in chatservice and Send does not return it.
	// Stamp gateway-observed time instead (milliseconds off server time, fine for display).
	// Exact server timestamps always arrive via GetHistory catch-up.
	event, err := json.Marshal(wsOutgoing{
		EventID:      response.GetEventId(),
		RoomID:       c.room,
		From:         c.from,
		Body:         incoming.Body,
		SentAtUnixMs: time.Now().UnixMilli(),
	})
	if err != nil {
		replyError(c, "failed to encode event")

		return
	}

	hub.broadcast <- roomMessage{room: c.room, data: event}
}

// writePump drains the client's send queue to the browser.
// It also pings every 30s so dead connections are detected by readPump.
func writePump(ctx context.Context, conn *websocket.Conn, c *Client) {
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()

	for {
		select {
		case data, ok := <-c.send:
			if !ok {
				conn.Close(websocket.StatusNormalClosure, "")
				return
			}

			wctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Write(wctx, websocket.MessageText, data)
			cancel()

			if err != nil {
				return
			}
		case <-ping.C:
			pctx, cancel := context.WithTimeout(ctx, writeTimeout)
			err := conn.Ping(pctx)
			cancel()

			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
