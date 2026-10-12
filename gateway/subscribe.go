package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
)

// subscriptions keeps one Subscribe loop per room, started on demand.
// Rooms are dynamic (browsers name them), so loops start lazily instead of upfront.
type subscriptions struct {
	mu     sync.Mutex
	active map[string]context.CancelFunc
	parent context.Context
	hub    *Hub
	chat   pb.ChatServiceClient
}

func newSubscriptions(parent context.Context, hub *Hub, chat pb.ChatServiceClient) *subscriptions {
	return &subscriptions{
		active: make(map[string]context.CancelFunc),
		parent: parent,
		hub:    hub,
		chat:   chat,
	}
}

// ensure starts the room loop once; later calls are no-ops.
// Loops follow the parent context: process lifetime in main, test scope in tests.
func (s *subscriptions) ensure(room string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.active[room]; ok {
		return
	}

	loopCtx, cancel := context.WithCancel(s.parent)
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

		hub.broadcast <- envelope{room: room, payload: data}
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
