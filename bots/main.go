package main

// bots spawns N fake WebSocket clients in one room that send and receive,
// then reports sent/received/dropped. Solo load test, no second human needed:
//
//	go run ./bots -n=50 -room=general
//
// Exit code is 1 when any message is dropped.
import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

type outgoing struct {
	Body string `json:"body"`
}

type incoming struct {
	EventID int64 `json:"event_id"`
}

type result struct {
	sent     int64
	received int64
}

// runBot dials one client, sends count messages spaced by interval,
// and counts broadcasts until the context expires.
func runBot(ctx context.Context, addr, room, name string, count int, interval time.Duration) result {
	var res result

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx,
		"ws://"+addr+"/ws?room="+room+"&from="+name, nil)
	cancel()

	if err != nil {
		log.Printf("bot %s: dial: %v", name, err)

		return res
	}

	defer conn.Close(websocket.StatusNormalClosure, "")

	// Warmup: the gateway opens its Subscribe stream on first register and the
	// handshake takes a moment. Sending immediately would store messages no
	// subscriber hears yet, so wait for the live path to be ready.
	select {
	case <-ctx.Done():
		return res
	case <-time.After(2 * time.Second):
	}

	// Reader lifetime is separate: it stops after the drain delay below,
	// not at the end of the whole run budget.
	readCtx, stopReading := context.WithCancel(ctx)

	// Reader: count every broadcast until the run ends.
	done := make(chan struct{})
	go func() {
		defer close(done)

		for {
			_, data, err := conn.Read(readCtx)
			if err != nil {
				return
			}

			var event incoming
			if err := json.Unmarshal(data, &event); err != nil {
				continue
			}
			if event.EventID != 0 {
				atomic.AddInt64(&res.received, 1)
			}
		}
	}()

	// Writer: spaced sends so the room sees a steady stream, not a burst.
	for i := 0; i < count; i++ {
		body, err := json.Marshal(outgoing{Body: fmt.Sprintf("%s-%d", name, i)})
		if err != nil {
			break
		}

		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = conn.Write(writeCtx, websocket.MessageText, body)
		cancel()

		if err != nil {
			break
		}

		atomic.AddInt64(&res.sent, 1)

		select {
		case <-ctx.Done():
			break
		case <-time.After(interval):
		}
	}

	// Drain: give the last broadcasts time to arrive, then stop the reader.
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
	}

	stopReading()
	<-done

	return res
}

func main() {
	addr := flag.String("addr", "localhost:8080", "gateway address")
	room := flag.String("room", "general", "room every bot joins")
	clients := flag.Int("n", 50, "number of fake clients")
	messages := flag.Int("m", 10, "messages each client sends")
	interval := flag.Duration("interval", 100*time.Millisecond, "gap between sends per client")
	timeout := flag.Duration("timeout", 2*time.Minute, "total run budget")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	var total result
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < *clients; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			// Stagger dials so N clients do not thunder the gateway at once.
			time.Sleep(time.Duration(i) * 10 * time.Millisecond)

			res := runBot(ctx, *addr, *room, fmt.Sprintf("bot-%d", i), *messages, *interval)

			mu.Lock()
			total.sent += res.sent
			total.received += res.received
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Every sent message is broadcast to every client, so each bot
	// should receive clients*messages events in total.
	expected := int64(*clients * *messages * *clients)
	dropped := expected - total.received

	fmt.Printf("sent=%d received=%d expected=%d dropped=%d\n",
		total.sent, total.received, expected, dropped)

	if dropped != 0 {
		os.Exit(1)
	}
}
