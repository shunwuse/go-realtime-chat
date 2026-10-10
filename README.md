# go-realtime-chat

A realtime chat to learn gRPC (unary + server streaming) and WebSocket in one Go monolith.

## Goals

- Learn gRPC unary (`Send`/`GetHistory`) and server streaming (`Subscribe`) in one project
- Learn WebSocket hub (broadcast, heartbeat, backpressure) via a browser chat room
- Stay solo-runnable: single repo, single `go.mod`, `docker-compose up` like a monolith
- Keep `proto/chat/v1/chat.proto` language-agnostic so `ChatService` can later be ported to Rust tonic unchanged

## Stack

- WebSocket: `github.com/coder/websocket` (hub pattern: register, unregister, broadcast; ping 30s, pong timeout 60s, per-client buffered send channel 256)
- gRPC: `google.golang.org/grpc` + `protobuf` (code generated with `buf`)
- Time: `int64` Unix milliseconds; errors via gRPC status codes (`InvalidArgument`, `NotFound`)
- Later: Redis Pub/Sub for cross-gateway fan-out (Phase 3 only)

## Project Layout

```text
proto/chat/v1/chat.proto  # contract: Send/GetHistory/Subscribe + ChatEvent (field numbers frozen)
gateway/main.go + hub.go  # :8080, WebSocket to browsers, gRPC client to chatservice
chatservice/main.go + server.go # :50051, pure gRPC, in-memory store first, Postgres later
bots/main.go              # fake WebSocket clients for solo testing (-n, -room)
docker-compose.yml        # gateway, chatservice, redis
```

Flow: `Browser --WebSocket--> gateway :8080 --gRPC--> chatservice :50051`

## Quick Start

```bash
go run ./chatservice &
go run ./gateway &
# open http://localhost:8080 twice (normal + incognito) and chat
```

```bash
docker-compose up
```

## Demo Script

```bash
# 1. History catch-up (reconnect gap fill)
# open WS with ?last_event_id=<n>, gateway calls GetHistory(since) before joining room

# 2. Solo load test: 50 fake clients in one room
go run ./bots -n=50 -room=general
# -> sent=... received=... dropped=0

# 3. Resilience: restart chatservice, gateway stream reconnects with backoff
```

## Acceptance Criteria

- `docker-compose up`, normal + incognito windows interoperate; restart chatservice, gateway stream reconnects
- `go run ./bots -n=100` for 10 min: no dropped messages, no goroutine leak (`goleak`)
- `docker-compose up --scale gateway=2`, clients on different gateway ports interoperate (Phase 3 via Redis; Phase 1-2 single gateway is OK)
- `go test ./...` passes, `ghz` reports p99 on Send/GetHistory

## Development

```bash
go list ./...
go vet ./...
go test ./...
```

Out of scope for now: real auth (stub JWT via interceptor only), typing indicator, read receipts, multi-room UI, client-streaming/bidi, K8s.
