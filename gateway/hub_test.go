package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	pb "example.com/chat/proto/gen/chat/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeChatServer is a minimal in-memory ChatService for gateway tests.
// It stores what Send receives so GetHistory can replay it.
type fakeChatServer struct {
	pb.UnimplementedChatServiceServer
	mu     sync.Mutex
	events []*pb.ChatEvent
	nextID atomic.Int64
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

// serve starts a real HTTP server wired to a fake chatservice.
// Each test gets a fresh hub and a fresh fake store.
func serve(t *testing.T) (string, pb.ChatServiceClient) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterChatServiceServer(srv, &fakeChatServer{})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial fake chatservice: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	chat := pb.NewChatServiceClient(conn)

	hub := NewHub()
	go hub.Run()
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		serveWS(hub, chat, w, request)
	}))
	t.Cleanup(httpServer.Close)

	url := "ws" + httpServer.URL[len("http"):] + "/ws"

	return url, chat
}

func dialWS(t *testing.T, url, room, from string) *websocket.Conn {
	t.Helper()

	return dialWSWith(t, url+"?room="+room+"&from="+from)
}

func dialWSWith(t *testing.T, fullURL string) *websocket.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, fullURL, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", fullURL, err)
	}

	return conn
}

func writeBody(t *testing.T, conn *websocket.Conn, body string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := json.Marshal(wsIncoming{Body: body})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func readEvent(t *testing.T, conn *websocket.Conn) wsOutgoing {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var event wsOutgoing
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}

	return event
}

// Two browsers in one room hear each other through Send + broadcast.
func TestRoomBroadcast(t *testing.T) {
	url, _ := serve(t)
	alice := dialWS(t, url, "general", "alice")
	defer alice.Close(websocket.StatusNormalClosure, "")
	bob := dialWS(t, url, "general", "bob")
	defer bob.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond) // let both register

	writeBody(t, alice, "hi")

	event := readEvent(t, alice)
	if event.Body != "hi" || event.From != "alice" || event.EventID != 1 {
		t.Fatalf("alice got %+v, want body hi from alice id 1", event)
	}

	event = readEvent(t, bob)
	if event.Body != "hi" || event.From != "alice" || event.EventID != 1 {
		t.Fatalf("bob got %+v, want body hi from alice id 1", event)
	}
}

// Closing one client does not disturb the other.
func TestClientLeave(t *testing.T) {
	url, _ := serve(t)
	alice := dialWS(t, url, "general", "alice")
	bob := dialWS(t, url, "general", "bob")
	defer bob.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond)

	alice.Close(websocket.StatusNormalClosure, "")
	time.Sleep(200 * time.Millisecond) // let unregister propagate

	writeBody(t, bob, "still here")

	event := readEvent(t, bob)
	if event.Body != "still here" {
		t.Fatalf("bob got %+v, want still here", event)
	}
}

// Reconnecting with last_event_id replays missed events before live traffic.
func TestHistoryCatchup(t *testing.T) {
	url, chat := serve(t)
	ctx := context.Background()

	for _, body := range []string{"one", "two"} {
		if _, err := chat.Send(ctx, &pb.SendRequest{RoomId: "general", From: "alice", Body: body}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	// Missed event 2 (exact server timestamp 7 proves it came from storage, not live echo).
	rejoin := dialWSWith(t, url+"?room=general&from=bob&last_event_id=1")
	defer rejoin.Close(websocket.StatusNormalClosure, "")

	event := readEvent(t, rejoin)
	if event.Body != "two" || event.SentAtUnixMs != 7 {
		t.Fatalf("catch-up got %+v, want body two with stored timestamp", event)
	}
}
