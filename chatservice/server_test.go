package main

import (
	"context"
	"net"
	"testing"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dial starts an in-memory gRPC server and returns a client for it.
// bufconn carries the traffic in memory, so no real port is used.
func dial(t *testing.T) pb.ChatServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterChatServiceServer(srv, &Server{})
	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewChatServiceClient(conn)
}

func TestSend_History(t *testing.T) {
	ctx := context.Background()
	c := dial(t)

	for i, body := range []string{"hello", "world", "again"} {
		resp, err := c.Send(ctx, &pb.SendRequest{RoomId: "general", From: "alice", Body: body})
		if err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
		if want := int64(i + 1); resp.GetEventId() != want {
			t.Fatalf("Send #%d event_id = %d, want %d", i, resp.GetEventId(), want)
		}
	}

	hist, err := c.GetHistory(ctx, &pb.GetHistoryRequest{RoomId: "general"})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	if len(hist.GetEvents()) != 3 {
		t.Fatalf("events = %d, want 3", len(hist.GetEvents()))
	}
	for i, ev := range hist.GetEvents() {
		if ev.GetEventId() != int64(i+1) {
			t.Fatalf("events[%d].event_id = %d, want %d", i, ev.GetEventId(), i+1)
		}
	}
}

func TestSend_Invalid(t *testing.T) {
	ctx := context.Background()
	c := dial(t)

	if _, err := c.Send(ctx, &pb.SendRequest{RoomId: "general", From: "alice"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Send empty body code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestHistory_Since(t *testing.T) {
	ctx := context.Background()
	c := dial(t)

	for _, body := range []string{"one", "two", "three"} {
		if _, err := c.Send(ctx, &pb.SendRequest{RoomId: "general", From: "bob", Body: body}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	hist, err := c.GetHistory(ctx, &pb.GetHistoryRequest{RoomId: "general", SinceEventId: 2})
	if err != nil {
		t.Fatalf("GetHistory: %v", err)
	}
	events := hist.GetEvents()
	if len(events) != 1 || events[0].GetBody() != "three" {
		t.Fatalf("since=2 got %d events, want only [three]", len(events))
	}
}

func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := dial(t)

	open := func() pb.ChatService_SubscribeClient {
		t.Helper()
		st, err := c.Subscribe(ctx, &pb.SubscribeRequest{RoomId: "general"})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		return st
	}
	s1, s2 := open(), open()

	// Let the server register both subscriptions before sending.
	time.Sleep(200 * time.Millisecond)

	if _, err := c.Send(ctx, &pb.SendRequest{RoomId: "general", From: "alice", Body: "live"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	for i, st := range []pb.ChatService_SubscribeClient{s1, s2} {
		recv := make(chan *pb.ChatEvent, 1)
		go func() {
			ev, err := st.Recv()
			if err != nil {
				return
			}
			recv <- ev
		}()
		select {
		case ev := <-recv:
			if ev.GetBody() != "live" {
				t.Fatalf("sub %d body = %q, want live", i, ev.GetBody())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("sub %d timed out waiting for event", i)
		}
	}
}
