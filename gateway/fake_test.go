package main

import (
	"context"
	"sync"
	"sync/atomic"

	pb "example.com/chat/proto/gen/chat/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeChatServer is a minimal in-memory ChatService for gateway tests.
// It stores what Send receives so GetHistory can replay it.
type fakeChatServer struct {
	pb.UnimplementedChatServiceServer
	mu          sync.Mutex
	events      []*pb.ChatEvent
	subscribers map[string]map[chan *pb.ChatEvent]struct{}
	nextID      atomic.Int64
}

func (f *fakeChatServer) Send(ctx context.Context, request *pb.SendRequest) (*pb.SendResponse, error) {
	if request.GetRoomId() == "" || request.GetFrom() == "" || request.GetBody() == "" {
		return nil, status.Error(codes.InvalidArgument, "room_id, from, body are required")
	}

	id := f.nextID.Add(1)
	event := &pb.ChatEvent{
		RoomId:       request.GetRoomId(),
		From:         request.GetFrom(),
		Body:         request.GetBody(),
		SentAtUnixMs: 7,
		EventId:      id,
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, event)

	for ch := range f.subscribers[request.GetRoomId()] {
		select {
		case ch <- event:
		default:
		}
	}

	return &pb.SendResponse{EventId: id}, nil
}

func (f *fakeChatServer) GetHistory(ctx context.Context, request *pb.GetHistoryRequest) (*pb.GetHistoryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var missed []*pb.ChatEvent
	for _, event := range f.events {
		if event.GetRoomId() == request.GetRoomId() && event.GetEventId() > request.GetSinceEventId() {
			missed = append(missed, event)
		}
	}

	limit := int(request.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if len(missed) > limit {
		missed = missed[len(missed)-limit:]
	}

	return &pb.GetHistoryResponse{Events: missed}, nil
}

func (f *fakeChatServer) Subscribe(request *pb.SubscribeRequest, stream pb.ChatService_SubscribeServer) error {
	ch := make(chan *pb.ChatEvent, 64)
	room := request.GetRoomId()

	f.mu.Lock()
	if f.subscribers == nil {
		f.subscribers = make(map[string]map[chan *pb.ChatEvent]struct{})
	}
	if f.subscribers[room] == nil {
		f.subscribers[room] = make(map[chan *pb.ChatEvent]struct{})
	}
	f.subscribers[room][ch] = struct{}{}
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		defer f.mu.Unlock()

		delete(f.subscribers[room], ch)
	}()

	for {
		select {
		case event := <-ch:
			if err := stream.Send(event); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}
