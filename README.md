# go-realtime-chat

A realtime chat to learn gRPC (unary + server streaming) and WebSocket in one Go monolith.

## Goals

- Learn gRPC unary (`Send`/`GetHistory`) and server streaming (`Subscribe`) in one project
- Learn WebSocket hub (broadcast, heartbeat, backpressure) via a browser chat room
- Stay solo-runnable: single repo, single `go.mod`, `docker compose up` like a monolith
- Keep `proto/chat/v1/chat.proto` language-agnostic so `ChatService` can later be ported to Rust tonic unchanged

## Stack

- WebSocket: `github.com/coder/websocket` (hub pattern: register, unregister, broadcast; ping 30s, pong timeout 60s, per-client buffered outbox 256)
- gRPC: `google.golang.org/grpc` + `protobuf` (code generated with `buf`)
- Time: `int64` Unix milliseconds; errors via gRPC status codes (`InvalidArgument`)
- Tests: `bufconn` + `httptest` + `goleak` (no leaked goroutines)
- Containers: multi-stage builds on `scratch` images

## Project Layout

```text
.
├── proto/chat/v1/chat.proto  # contract: Send/GetHistory/Subscribe + ChatEvent (numbers frozen)
├── proto/gen/                # generated code (committed so services import it directly)
├── chatservice/
│   ├── main.go               # :50051 bootstrap: listen, interceptors, serve
│   ├── server.go             # in-memory store: Send / GetHistory / Subscribe + fan-out
│   ├── server_test.go        # bufconn tests (no real ports)
│   └── Dockerfile            # scratch image
├── gateway/
│   ├── main.go               # :8080 wiring only: gRPC client, hub, routes
│   ├── ws.go                 # browser side: serveWS, pumps, Send chain, catch-up
│   ├── subscribe.go          # chatservice side: per-room Subscribe loops with backoff
│   ├── hub.go                # room switchboard: register, broadcast, slow-client drop
│   ├── page.go + static/     # manual test page (embedded via go:embed)
│   ├── *_test.go             # httptest + fake chatservice + goleak
│   └── Dockerfile            # scratch image
├── bots/main.go              # fake WebSocket clients for solo load tests
└── docker-compose.yml        # gateway + chatservice
```

## Flow

Three roles, two languages. Browsers speak WebSocket, chatservice speaks gRPC,
and the gateway is the only one fluent in both.

```text
Browser --WebSocket--> gateway :8080 --gRPC--> chatservice :50051
```

- **Store** (unary, one round trip): browser JSON → `Send` → stored with an
  `event_id`. Failures reply to the sender only; the room never sees them.
- **Push** (streaming, long-lived): stored events fan out to per-room
  `Subscribe` streams → gateway hub → every browser in the room, sender included.
- **Catch up** (unary, on reconnect): the browser sends `?last_event_id=<n>`,
  the gateway replays missed events via `GetHistory` before joining live traffic.

## Quick Start

Run one stack at a time: local `go run` or compose, never both (ports collide
and you will talk to the wrong one). Pick either block below, not both.

```bash
go run ./chatservice &
go run ./gateway &
```

```bash
docker compose up --build
```

Then open `http://localhost:8080` in two tabs (names are random per load) and chat.
Join first, then send: messages sent before joining go nowhere.

## Manual Test

1. **Interoperate**: two tabs, same room. Both sides
   receive the same message with the same `event_id`.
2. **Reconnect**: note the `last` field, refresh, join again. Missed messages
   replay before live traffic resumes.
3. **Newcomer**: a third tab joins with an empty `last` field and sees
   recent history (up to 200 events).
4. **Resilience**: restart chatservice. New messages fail while it is down;
   after restart the gateway stream reconnects with backoff and traffic resumes
   without restarting the gateway.

## Load Test

```bash
# with both services running (see Quick Start above)
go run ./bots -n=50 -room=general
# -> sent=500 received=25000 expected=25000 dropped=0
```

| Flag       | Default         | Purpose                              |
| ---------- | --------------- | ------------------------------------ |
| `-n`       | `50`            | Number of fake clients               |
| `-m`       | `10`            | Messages each client sends           |
| `-room`    | `general`       | Room every bot joins                 |
| `-interval`| `100ms`         | Gap between sends per client         |
| `-timeout` | `2m`            | Total run budget                     |

Every message is broadcast to every client, so expected receives equal
clients squared times messages. Any shortfall exits nonzero. Bots wait 2s
after dialing so the gateway Subscribe stream is ready before the first send.

## Benchmark (local, in-memory store)

```bash
ghz --insecure --proto proto/chat/v1/chat.proto \
  --call chat.v1.ChatService.Send \
  -d '{"room_id":"bench","from":"ghz","body":"hi"}' \
  -c 100 -n 10000 localhost:50051

ghz --insecure --proto proto/chat/v1/chat.proto \
  --call chat.v1.ChatService.GetHistory \
  -d '{"room_id":"bench","limit":50}' \
  -c 50 -n 5000 localhost:50051
```

| Call         | RPS    | p50     | p99     | Notes                          |
| ------------ | ------ | ------- | ------- | ------------------------------ |
| Send         | 24.4k  | 2.7ms   | 11.8ms  | 100 conn, 10k calls, all OK    |
| GetHistory   | 2.6k   | 18.6ms  | 32.7ms  | 50 conn, 5k calls, room holds 10k events |

GetHistory scans the whole room before taking the limit, so it slows as rooms
grow. A future Postgres index is the fix; these numbers are the baseline to
compare against.

## Development

```bash
go list ./...
go vet ./...
go test ./...
```

Out of scope for now: real auth (stub JWT via interceptor only), typing indicator, read receipts, multi-room UI, client-streaming/bidi, K8s.
