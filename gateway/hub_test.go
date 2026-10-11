package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// serve starts a real HTTP server with a fresh hub for each test.
func serve(t *testing.T) string {
	t.Helper()
	hub := NewHub()
	go hub.Run()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveWS(hub, w, r)
	}))
	t.Cleanup(srv.Close)
	return "ws" + srv.URL[len("http"):] + "/ws"
}

func dialWS(t *testing.T, url, room, from string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url+"?room="+room+"&from="+from, nil)
	if err != nil {
		t.Fatalf("dial %s/%s: %v", room, from, err)
	}
	return conn
}

func readOne(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(data)
}

// Two browsers in one room hear each other (echo broadcast, no gRPC yet).
func TestRoomBroadcast(t *testing.T) {
	url := serve(t)
	a := dialWS(t, url, "general", "alice")
	defer a.Close(websocket.StatusNormalClosure, "")
	b := dialWS(t, url, "general", "bob")
	defer b.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond) // let both register

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Write(ctx, websocket.MessageText, []byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := readOne(t, a); got != "hi" {
		t.Fatalf("alice got %q, want hi", got)
	}
	if got := readOne(t, b); got != "hi" {
		t.Fatalf("bob got %q, want hi", got)
	}
}

// Closing one client does not disturb the other.
func TestClientLeave(t *testing.T) {
	url := serve(t)
	a := dialWS(t, url, "general", "alice")
	b := dialWS(t, url, "general", "bob")
	defer b.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond)

	a.Close(websocket.StatusNormalClosure, "")
	time.Sleep(200 * time.Millisecond) // let unregister propagate

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Write(ctx, websocket.MessageText, []byte("still here")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := readOne(t, b); got != "still here" {
		t.Fatalf("bob got %q, want still here", got)
	}
}
