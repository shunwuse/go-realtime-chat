package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
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
	// Reconnect backoff for Subscribe streams: start at 1s, double to 30s max.
	// Streaming is never retried per-message; the gap is covered by GetHistory catch-up.
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
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

	subs := newSubscriptions(hub, chat)

	http.HandleFunc("/", servePage)
	http.HandleFunc("/ws", func(w http.ResponseWriter, request *http.Request) {
		serveWS(hub, chat, subs, w, request)
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
func serveWS(hub *Hub, chat pb.ChatServiceClient, subs *subscriptions, w http.ResponseWriter, r *http.Request) {
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

	// First browser in this room opens the Subscribe stream; the rest share it.
	subs.ensure(room)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go writePump(ctx, conn, c)
	readPump(ctx, conn, hub, chat, c)
}

// subscriptions keeps one Subscribe loop per room, started on demand.
// Rooms are dynamic (browsers name them), so loops start lazily instead of upfront.
type subscriptions struct {
	mu     sync.Mutex
	active map[string]context.CancelFunc
	hub    *Hub
	chat   pb.ChatServiceClient
}

func newSubscriptions(hub *Hub, chat pb.ChatServiceClient) *subscriptions {
	return &subscriptions{
		active: make(map[string]context.CancelFunc),
		hub:    hub,
		chat:   chat,
	}
}

// ensure starts the room loop once; later calls are no-ops.
// Loops live on context.Background: they outlive any single browser.
func (s *subscriptions) ensure(room string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.active[room]; ok {
		return
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	s.active[room] = cancel

	go subscribeLoop(loopCtx, s.hub, s.chat, room)
}

// subscribeLoop holds one Subscribe stream and fans its events into the hub.
// On stream failure it backs off (1s doubling to 30s) and re-subscribes;
// the gap is covered by GetHistory catch-up, so events are never retried here.
func subscribeLoop(ctx context.Context, hub *Hub, chat pb.ChatServiceClient, room string) {
	backoff := minBackoff

	for {
		stream, err := chat.Subscribe(ctx, &pb.SubscribeRequest{RoomId: room})
		if err != nil {
			log.Printf("subscribe %s: %v (retry in %s)", room, err, backoff)

			if !sleepOrDone(ctx, backoff) {
				return
			}

			backoff = nextBackoff(backoff)

			continue
		}

		// Connected: reset backoff, pump until the stream breaks or we shut down.
		backoff = minBackoff

		if !pumpStream(ctx, hub, room, stream) {
			return
		}

		log.Printf("subscribe %s broke (retry in %s)", room, backoff)

		if !sleepOrDone(ctx, backoff) {
			return
		}

		backoff = nextBackoff(backoff)
	}
}

// pumpStream forwards one stream into the hub broadcast.
// It returns false only on shutdown; a broken stream returns true to reconnect.
func pumpStream(ctx context.Context, hub *Hub, room string, stream pb.ChatService_SubscribeClient) bool {
	for {
		event, err := stream.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}

		data, err := json.Marshal(encodeEvent(event))
		if err != nil {
			continue
		}

		hub.broadcast <- roomMessage{room: room, data: data}
	}
}

// sleepOrDone waits out the backoff but returns early on shutdown.
func sleepOrDone(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// nextBackoff doubles until the 30s ceiling.
func nextBackoff(current time.Duration) time.Duration {
	if current >= maxBackoff {
		return maxBackoff
	}

	return current * 2
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

		handleMessage(ctx, chat, c, data)
	}
}

// handleMessage stores one browser message via Send and reports the result.
// It never broadcasts: the Subscribe loop is the single broadcast path,
// so the sender gets exactly one copy like everyone else.
// Failures go back to the sender only; the room never sees them.
func handleMessage(ctx context.Context, chat pb.ChatServiceClient, c *Client, data []byte) {
	var incoming wsIncoming
	if err := json.Unmarshal(data, &incoming); err != nil {
		replyError(c, "message must be JSON like {\"body\":\"hi\"}")

		return
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	_, err := chat.Send(rpcCtx, &pb.SendRequest{
		RoomId: c.room,
		From:   c.from,
		Body:   incoming.Body,
	})
	cancel()

	if err != nil {
		replyError(c, err.Error())

		return
	}
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
