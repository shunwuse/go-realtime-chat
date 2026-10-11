package main

import (
	"context"
	"log"
	"net"
	"time"

	pb "example.com/chat/proto/gen/chat/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen :50051: %v", err)
	}

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingInterceptor, stubAuthInterceptor),
	)
	pb.RegisterChatServiceServer(srv, &Server{})

	log.Println("chatservice listening on :50051")
	if err := srv.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// loggingInterceptor records method, latency and error for every unary call.
func loggingInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, request)

	log.Printf("method=%s duration=%s err=%v", info.FullMethod, time.Since(start), err)

	return resp, err
}

// stubAuthInterceptor logs the authorization header but never blocks.
// Real JWT verification is out of scope for the MVP.
func stubAuthInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		log.Printf("method=%s authorization=%v", info.FullMethod, md.Get("authorization"))
	}

	return handler(ctx, request)
}
