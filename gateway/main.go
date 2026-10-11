package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

const (
	// Heartbeat: ping every 30s, read must succeed within 60s, writes within 10s.
	pingInterval = 30 * time.Second
	pongTimeout  = 60 * time.Second
	writeTimeout = 10 * time.Second
)

func main() {
	hub := NewHub()
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	})

	log.Println("gateway listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// serveWS upgrades one browser connection and joins it to a room.
// Query: /ws?room=<room>&from=<name>. No history yet (step 6 wires gRPC).
func serveWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
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
	hub.register <- c

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go writePump(ctx, conn, c)
	readPump(ctx, conn, hub, c)
}

// readPump forwards browser text messages to the hub broadcast.
// It returns when the browser goes away, then unregisters the client.
func readPump(ctx context.Context, conn *websocket.Conn, hub *Hub, c *Client) {
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

		hub.broadcast <- roomMessage{room: c.room, data: data}
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
