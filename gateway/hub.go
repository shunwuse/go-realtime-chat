package main

import (
	"sync"
)

// Client is one browser WebSocket connection.
// outbox is buffered (256): a slow client is dropped, never blocking the Hub.
type Client struct {
	hub    *Hub
	room   string
	from   string
	outbox chan []byte // hub --> browser: queued frames waiting to be written
}

// envelope is one broadcast unit: room is the address, payload is the letter.
// The Hub speaks envelopes internally; translation to bytes happens at the edges.
type envelope struct {
	room    string
	payload []byte
}

// Hub is the central switchboard: room -> member clients.
// All state is owned by Run's goroutine; outsiders talk via channels only.
type Hub struct {
	register   chan *Client  // browsers --> hub: someone joins a room
	unregister chan *Client  // pumps --> hub: someone disconnects
	broadcast  chan envelope // loops --> hub: one event to fan out
	rooms      map[string]map[*Client]struct{}
	done       chan struct{}
	stopOnce   sync.Once
}

func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan envelope, 256),
		rooms:      make(map[string]map[*Client]struct{}),
		done:       make(chan struct{}),
	}
}

// Stop shuts the Run loop down. Production never calls it (process lifetime);
// tests call it in cleanup so goleak sees no leftover Hub.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() {
		close(h.done)
	})
}

// Run is the Hub's event loop. Run it once: go hub.Run().
func (h *Hub) Run() {
	for {
		select {
		case <-h.done:
			return
		case client := <-h.register:
			if h.rooms[client.room] == nil {
				h.rooms[client.room] = make(map[*Client]struct{})
			}
			h.rooms[client.room][client] = struct{}{}

		case client := <-h.unregister:
			h.remove(client)

		case msg := <-h.broadcast:
			for client := range h.rooms[msg.room] {
				select {
				case client.outbox <- msg.payload:
				default:
					// Slow client: drop it so one laggard
					// never blocks the whole room.
					h.remove(client)
				}
			}
		}
	}
}

// remove detaches a client and closes its outbox channel.
// Only Run calls it, so no lock is needed.
func (h *Hub) remove(client *Client) {
	members := h.rooms[client.room]
	if members == nil {
		return
	}
	if _, ok := members[client]; !ok {
		return
	}

	delete(members, client)

	close(client.outbox)

	if len(members) == 0 {
		delete(h.rooms, client.room)
	}
}
