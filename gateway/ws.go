package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
	"github.com/coder/websocket"
)

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
func serveWS(hub *Hub, chat pb.ChatServiceClient, roomStreams *subscriptions, w http.ResponseWriter, r *http.Request) {
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

	client := &Client{
		hub:    hub,
		room:   room,
		from:   from,
		outbox: make(chan []byte, 256),
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

		for _, payload := range history {
			select {
			case client.outbox <- payload:
			default:
			}
		}
	}

	// First browser in this room opens the Subscribe stream; the rest share it.
	// Started before register so the loop is already dialing when the client
	// joins. A message sent within the handshake window can still be missed;
	// browsers cover it with last_event_id catch-up on their next join.
	roomStreams.ensure(room)

	hub.register <- client

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go writePump(ctx, conn, client)
	readPump(ctx, conn, hub, chat, client)
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
func replyError(client *Client, message string) {
	data, err := json.Marshal(wsError{Error: message})
	if err != nil {
		return
	}

	select {
	case client.outbox <- data:
	default:
	}
}

// readPump forwards browser text messages to chatservice, then broadcasts results.
// It returns when the browser goes away, then unregisters the client.
func readPump(ctx context.Context, conn *websocket.Conn, hub *Hub, chat pb.ChatServiceClient, client *Client) {
	defer func() {
		hub.unregister <- client
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

		handleMessage(ctx, chat, client, data)
	}
}

// handleMessage stores one browser message via Send and reports the result.
// It never broadcasts: the Subscribe loop is the single broadcast path,
// so the sender gets exactly one copy like everyone else.
// Failures go back to the sender only; the room never sees them.
func handleMessage(ctx context.Context, chat pb.ChatServiceClient, client *Client, data []byte) {
	var incoming wsIncoming
	if err := json.Unmarshal(data, &incoming); err != nil {
		replyError(client, "message must be JSON like {\"body\":\"hi\"}")

		return
	}

	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	_, err := chat.Send(rpcCtx, &pb.SendRequest{
		RoomId: client.room,
		From:   client.from,
		Body:   incoming.Body,
	})
	cancel()

	if err != nil {
		replyError(client, err.Error())

		return
	}
}

// writePump drains the client's send queue to the browser.
// It also pings every 30s so dead connections are detected by readPump.
func writePump(ctx context.Context, conn *websocket.Conn, client *Client) {
	ping := time.NewTicker(pingInterval)
	defer ping.Stop()

	for {
		select {
		case data, ok := <-client.outbox:
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
