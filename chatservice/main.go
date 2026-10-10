package main

import (
	"log"
	"net"

	pb "example.com/chat/proto/gen/chat/v1"
	"google.golang.org/grpc"
)

func main() {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen :50051: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterChatServiceServer(srv, &Server{})

	log.Println("chatservice listening on :50051")
	if err := srv.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
