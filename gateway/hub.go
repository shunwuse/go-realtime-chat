package main

import (
	"sync"
)

// Client is one browser WebSocket connection.
// send is buffered (256): a slow client is dropped, never blocking the Hub.
type Client struct {
	hub  *Hub
	room string
	from string
	send chan []byte
}

// roomMessage is one broadcast unit: raw bytes to every client in room.
type roomMessage struct {
	room string
	data []byte
}

// Hub is the central switchboard: room -> member clients.
// All state is owned by Run's goroutine; outsiders talk via channels only.
type Hub struct {
	register   chan *Client
	unregister chan *Client
	broadcast  chan roomMessage
	rooms      map[string]map[*Client]struct{}
	done       chan struct{}
	stopOnce   sync.Once
}

func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan roomMessage, 256),
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
		case c := <-h.register:
			if h.rooms[c.room] == nil {
				h.rooms[c.room] = make(map[*Client]struct{})
			}
			h.rooms[c.room][c] = struct{}{}

		case c := <-h.unregister:
			h.remove(c)

		case m := <-h.broadcast:
			for c := range h.rooms[m.room] {
				select {
				case c.send <- m.data:
				default:
					// Slow client: drop it so one laggard
					// never blocks the whole room.
					h.remove(c)
				}
			}
		}
	}
}

// remove detaches a client and closes its send channel.
// Only Run calls it, so no lock is needed.
func (h *Hub) remove(c *Client) {
	members := h.rooms[c.room]
	if members == nil {
		return
	}
	if _, ok := members[c]; !ok {
		return
	}

	delete(members, c)

	close(c.send)

	if len(members) == 0 {
		delete(h.rooms, c.room)
	}
}
