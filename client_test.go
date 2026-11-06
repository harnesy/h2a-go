package h2a

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func dial(t *testing.T, n *fakeNode, cfg Config) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// The sign-in: the subprotocol, no Origin, the token in the hello only, the
// welcome read.
func TestDialSignsIn(t *testing.T) {
	n := newFakeNode(t)
	c := dial(t, n, n.config())
	w := c.Welcome()
	if w.Project != "App" || len(w.To) != 2 || w.To[0].Address != "lead" || w.Limits.TextBytes != 16384 {
		t.Fatalf("welcome %+v", w)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	h := n.hellos[0]
	if h.V != "1" || h.Token != goodToken || len(h.Can) != 1 || h.Can[0] != "text" {
		t.Fatalf("hello %+v", h)
	}
	for k, v := range n.headers[0] {
		if strings.Contains(strings.Join(v, " "), goodToken) || strings.EqualFold(k, "Origin") {
			t.Fatalf("header %s: %v", k, v)
		}
	}
	if strings.Contains(n.urls[0], goodToken) {
		t.Fatalf("the token in the URL: %s", n.urls[0])
	}
}

// A message is accepted; a disconnect before its accepted resends the same
// ID after the reconnect, and the node delivers it once.
func TestSendSameIDAfterADrop(t *testing.T) {
	fastPauses(t)
	n := newFakeNode(t)
	var dropped atomic.Bool
	n.onMessage = func(n *fakeNode, c *websocket.Conn, m messageFrame) bool {
		if dropped.CompareAndSwap(false, true) {
			c.CloseNow() // gone before the accepted
			return true
		}
		return false
	}
	c := dial(t, n, n.config())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	acc, err := c.Send(ctx, Message{ID: "task-1", To: "lead", Text: "the release is built?"})
	if err != nil || acc.ID != "task-1" || acc.Conversation == "" {
		t.Fatalf("send: %+v %v", acc, err)
	}
	again, err := c.Send(ctx, Message{ID: "task-1", To: "lead", Text: "the release is built?"})
	if err != nil || again.Conversation != acc.Conversation {
		t.Fatalf("a repeated ID: %+v %v", again, err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.delivered["task-1"] != 1 || n.conns != 2 {
		t.Fatalf("delivered %d times over %d connections", n.delivered["task-1"], n.conns)
	}
}

// Replies come in order, each acknowledged and kept in the Cursor; after a
// drop the hello asks from the cursor, nothing comes twice; a handler's
// error is returned and the next Run gets that reply again.
func TestRunAcksAndResumes(t *testing.T) {
	fastPauses(t)
	n := newFakeNode(t)
	cur := FileCursor(filepath.Join(t.TempDir(), "seq"))
	cfg := n.config()
	cfg.Cursor = cur
	c := dial(t, n, cfg)
	var mu sync.Mutex
	var got []string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run := func(fail string) error {
		return c.Run(ctx, func(_ context.Context, m Reply) error {
			if m.Text == fail {
				return errors.New("not now")
			}
			mu.Lock()
			got = append(got, fmt.Sprintf("%d:%s", m.Seq, m.Text))
			done := len(got)
			mu.Unlock()
			if done == 3 {
				cancel()
			}
			return nil
		})
	}
	n.reply("one")
	n.reply("two")
	if err := run("two"); err == nil || err.Error() != "not now" {
		t.Fatalf("a handler's error: %v", err)
	}
	waitFor(t, "ack 1", func() bool { n.mu.Lock(); defer n.mu.Unlock(); return n.acked == 1 })
	if s, _ := cur.Load(); s != 1 {
		t.Fatalf("cursor %d", s)
	}
	n.drop() // «two» comes again on the next connection: it is not acknowledged
	waitFor(t, "a reconnect", func() bool { return n.count() == 2 })
	n.reply("three")
	go func() { time.Sleep(50 * time.Millisecond); n.reply("unused") }()
	if err := run(""); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(got, ",") != "1:one,2:two,3:three" {
		t.Fatalf("replies %v", got)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.hellos[1].ResumeAfter != 1 {
		t.Fatalf("the second hello's resume_after %d", n.hellos[1].ResumeAfter)
	}
}

// The node's ping gets a pong, with no handler.
func TestPingGetsPong(t *testing.T) {
	n := newFakeNode(t)
	dial(t, n, n.config())
	n.mu.Lock()
	c := n.live
	n.mu.Unlock()
	n.send(c, map[string]string{"type": "ping"})
	waitFor(t, "a pong", func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		for _, f := range n.frames {
			if f.Type == "pong" {
				return true
			}
		}
		return false
	})
}

// A text over the welcome's text_bytes is refused here, never sent.
func TestTooBigIsRefusedLocally(t *testing.T) {
	n := newFakeNode(t)
	n.textBytes = 10
	c := dial(t, n, n.config())
	_, err := c.Send(context.Background(), Message{To: "lead", Text: strings.Repeat("x", 11)})
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeTooBig {
		t.Fatalf("send: %v", err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.frames) != 0 {
		t.Fatalf("sent %v", n.frames)
	}
}

// h2a_rate on a frame: Send waits retry_after and sends the same frame once
// more; h2a_daily goes to the caller with until.
func TestSendRateAndDaily(t *testing.T) {
	n := newFakeNode(t)
	var tries atomic.Int32
	n.onMessage = func(n *fakeNode, c *websocket.Conn, m messageFrame) bool {
		try := tries.Add(1)
		switch {
		case m.ID == "r" && try == 1:
			n.send(c, map[string]any{"type": "error", "re": "r", "code": CodeRate, "text": "x", "retry_after": 0})
			return true
		case m.ID == "d":
			n.send(c, map[string]any{"type": "error", "re": "d", "code": CodeDaily, "text": "x", "until": "2026-11-07T00:00:00Z"})
			return true
		}
		return false
	}
	c := dial(t, n, n.config())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Send(ctx, Message{ID: "r", To: "lead", Text: "x"}); err != nil || tries.Load() != 2 {
		t.Fatalf("rate: %v after %d tries", err, tries.Load())
	}
	_, err := c.Send(ctx, Message{ID: "d", To: "lead", Text: "x"})
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeDaily || e.Until.IsZero() {
		t.Fatalf("daily: %v", err)
	}
}

// Every close and refused hello does what the contract's «Client» column
// says: stop (no reconnect), or reconnect after the pause.
func TestClosesFollowTheClientColumn(t *testing.T) {
	fastPauses(t)
	for _, r := range rules {
		if r.On == onFrame {
			continue
		}
		t.Run(fmt.Sprintf("%s_%d", r.Code, r.Close), func(t *testing.T) {
			n := newFakeNode(t)
			c := dial(t, n, n.config())
			n.mu.Lock()
			conn := n.live
			n.mu.Unlock()
			switch {
			case r.Close == 1006:
				conn.CloseNow()
			case r.Code == "":
				conn.Close(websocket.StatusCode(r.Close), "")
			default:
				n.closeWith(conn, r.Code, r.Close)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			switch r.Client {
			case MoveStop:
				err := c.Run(ctx, func(context.Context, Reply) error { return nil })
				var e *Error
				if r.Code == CodeReplaced {
					if !errors.Is(err, ErrReplaced) {
						t.Fatalf("run: %v", err)
					}
				} else if !errors.As(err, &e) || e.Code != r.Code {
					t.Fatalf("run: %v", err)
				}
				time.Sleep(60 * time.Millisecond)
				if n.count() != 1 {
					t.Fatalf("reconnected after stop: %d connections", n.count())
				}
			default:
				waitFor(t, "a reconnect", func() bool { return n.count() == 2 })
			}
		})
	}
}

// A refused hello: Dial returns the *Error; a stop code on a reconnect
// stops; h2a_rate on a hello is waited out, then the client signs in.
func TestRefusedHellos(t *testing.T) {
	fastPauses(t)
	n := newFakeNode(t)
	cfg := n.config()
	cfg.Token = "h2a_wrong"
	_, err := Dial(context.Background(), cfg)
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeAuth || e.Close != 4401 {
		t.Fatalf("a wrong token: %v", err)
	}
	c := dial(t, n, n.config())
	n.mu.Lock()
	n.refuse = []func(*websocket.Conn) bool{
		func(conn *websocket.Conn) bool {
			n.send(conn, map[string]any{"type": "error", "code": CodeRate, "text": "x", "retry_after": 0})
			conn.Close(4429, "")
			return true
		},
	}
	n.mu.Unlock()
	n.drop()
	waitFor(t, "signed in after the shut-out", func() bool { return n.count() == 3 && c.Welcome().Project == "App" })
	n.mu.Lock()
	n.refuse = []func(*websocket.Conn) bool{func(conn *websocket.Conn) bool { n.closeWith(conn, CodeOff, 4403); return true }}
	n.mu.Unlock()
	n.drop()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Run(ctx, func(context.Context, Reply) error { return nil }); !errors.As(err, &e) || e.Code != CodeOff {
		t.Fatalf("off on a reconnect: %v", err)
	}
}

// The pause grows ×2 from the first to the last, each within ±20 %, and a
// welcome resets it.
func TestBackoffGrows(t *testing.T) {
	b := &backoff{First: time.Second, Max: time.Minute, Factor: 2, Jitter: 0.2}
	for _, want := range []float64{1, 2, 4, 8, 16, 32, 60, 60} {
		d := b.pause().Seconds()
		if d < want*0.8 || d > want*1.2 {
			t.Fatalf("pause %.2f s, want %.0f ±20 %%", d, want)
		}
	}
	b.reset()
	if d := b.pause().Seconds(); d < 0.8 || d > 1.2 {
		t.Fatalf("after a reset %.2f s", d)
	}
}

// The token is in no error and no log line of the package.
func TestTokenNeverLeaks(t *testing.T) {
	fastPauses(t)
	var logs bytes.Buffer
	lg := log.New(&logs, "", 0)
	n := newFakeNode(t)
	cfg := n.config()
	cfg.Logf = lg.Printf
	var errs []error
	bad := cfg
	bad.Token = goodToken + "x"
	_, err := Dial(context.Background(), bad)
	errs = append(errs, err)
	bad.URL = "ws://127.0.0.1:1/h2a/v1"
	_, err = Dial(context.Background(), bad)
	errs = append(errs, err)
	c := dial(t, n, cfg)
	_, err = c.Send(context.Background(), Message{To: "lead", Text: strings.Repeat("x", 20000)})
	errs = append(errs, err)
	n.drop()
	waitFor(t, "a reconnect", func() bool { return n.count() == 3 })
	n.mu.Lock()
	conn := n.live
	n.mu.Unlock()
	n.closeWith(conn, CodeAuth, 4401)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	errs = append(errs, c.Run(ctx, func(context.Context, Reply) error { return nil }))
	for _, err := range errs {
		if err == nil {
			t.Fatal("an error expected")
		}
		if strings.Contains(err.Error(), "SECRET") || strings.Contains(fmt.Sprintf("%#v %+v", err, err), "SECRET") {
			t.Fatalf("the token in an error: %v", err)
		}
	}
	if strings.Contains(logs.String(), "SECRET") || logs.Len() == 0 {
		t.Fatalf("logs: %q", logs.String())
	}
	if s := fmt.Sprintf("%#v", helloFrame{Token: goodToken}); strings.Contains(s, "SECRET") {
		t.Fatalf("%%#v of a hello: %s", s)
	}
}

// Close ends Send and Run with ErrClosed.
func TestClose(t *testing.T) {
	n := newFakeNode(t)
	c := dial(t, n, n.config())
	c.Close()
	if err := c.Run(context.Background(), func(context.Context, Reply) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("run after close: %v", err)
	}
	if _, err := c.Send(context.Background(), Message{To: "lead", Text: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("send after close: %v", err)
	}
}

// Review n2: the URL is wss:// (ws:// only to this machine), with no user,
// password, query or fragment: the token goes in the hello alone.
func TestDialRefusesAnUnsafeURL(t *testing.T) {
	for _, u := range []string{
		"ws://example.com/h2a/v1",
		"http://127.0.0.1/h2a/v1",
		"wss://user:pass@example.com/h2a/v1",
		"wss://example.com/h2a/v1?token=h2a_x",
		"wss://example.com/h2a/v1#x",
	} {
		if _, err := Dial(context.Background(), Config{URL: u, Token: goodToken}); err == nil || strings.Contains(err.Error(), goodToken) {
			t.Errorf("%s: %v", u, err)
		}
	}
	for _, u := range []string{"wss://example.com/h2a/v1", "ws://127.0.0.1:1/h2a/v1", "ws://localhost:1/h2a/v1", "ws://[::1]:1/h2a/v1"} {
		if err := checkURL(u); err != nil {
			t.Errorf("%s refused: %v", u, err)
		}
	}
}

// Review n5: one Run per client at a time.
func TestRunIsOneAtATime(t *testing.T) {
	n := newFakeNode(t)
	c, err := Dial(context.Background(), n.config())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- c.Run(ctx, func(context.Context, Reply) error { return nil }) }()
	deadline := time.Now().Add(2 * time.Second)
	for !c.running.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := c.Run(context.Background(), func(context.Context, Reply) error { return nil }); !errors.Is(err, ErrRunning) {
		t.Fatalf("a second Run: %v", err)
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("the first Run: %v", err)
	}
}
