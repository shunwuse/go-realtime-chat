package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedChatServiceServer
	mu     sync.Mutex
	rooms  map[string][]*pb.ChatEvent
	nextID atomic.Int64 // atomic 遞增，當 event_id
}

func (s *Server) Send(ctx context.Context, request *pb.SendRequest) (*pb.SendResponse, error) {
	if request.GetRoomId() == "" {
		return nil, status.Error(codes.InvalidArgument, "room_id is required")
	}

	if request.GetFrom() == "" {
		return nil, status.Error(codes.InvalidArgument, "from is required")
	}

	if request.GetBody() == "" {
		return nil, status.Error(codes.InvalidArgument, "body is required")
	}

	id := s.nextID.Add(1)

	event := &pb.ChatEvent{
		RoomId:       request.GetRoomId(),
		From:         request.GetFrom(),
		Body:         request.GetBody(),
		SentAtUnixMs: time.Now().UnixMilli(),
		EventId:      id,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rooms == nil {
		s.rooms = make(map[string][]*pb.ChatEvent)
	}
	s.rooms[request.GetRoomId()] = append(s.rooms[request.GetRoomId()], event)

	return &pb.SendResponse{
		EventId: id,
	}, nil
}

func (s *Server) GetHistory(ctx context.Context, request *pb.GetHistoryRequest) (*pb.GetHistoryResponse, error) {
	limit := int(request.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var missed []*pb.ChatEvent
	for _, event := range s.rooms[request.GetRoomId()] {
		if event.GetEventId() > request.GetSinceEventId() {
			missed = append(missed, event)
		}
	}

	if len(missed) > limit {
		missed = missed[len(missed)-limit:]
	}

	return &pb.GetHistoryResponse{Events: missed}, nil
}
