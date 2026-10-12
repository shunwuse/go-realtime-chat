package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
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

	roomStreams := newSubscriptions(context.Background(), hub, chat)

	http.HandleFunc("/", servePage)
	http.HandleFunc("/ws", func(w http.ResponseWriter, request *http.Request) {
		serveWS(hub, chat, roomStreams, w, request)
	})

	log.Println("gateway listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
