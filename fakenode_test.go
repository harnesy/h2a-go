package h2a

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// The token of the fake node's pass: a test checks it never leaks.
const goodToken = "h2a_SECRETtokenSECRETtokenSECRET0123456789ab"

// fakeNode is a node of the contract for the tests: it signs a hello in,
// answers messages, sends replies, and does what a test asks of the next
// connection (refuse the hello, drop, close with a code).
type fakeNode struct {
	t   *testing.T
	srv *httptest.Server

	mu        sync.Mutex
	hellos    []helloFrame
	frames    []frame // every frame the service sent after its hello
	acked     int64
	delivered map[string]int // messages delivered to the «agent», by id
	conv      map[string]string
	queue     []Reply // the replies not yet acknowledged
	nextSeq   int64
	conns     int
	live      *websocket.Conn
	headers   []http.Header
	urls      []string

	// Per test: what the next hellos get (one entry per connection; empty:
	// a welcome), and a hook on each message frame.
	refuse    []func(c *websocket.Conn) bool                            // true: the connection was refused
	onMessage func(n *fakeNode, c *websocket.Conn, m messageFrame) bool // true: handled
	textBytes int
}

func newFakeNode(t *testing.T) *fakeNode {
	n := &fakeNode{t: t, delivered: map[string]int{}, conv: map[string]string{}, textBytes: 16384}
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(n.srv.Close)
	return n
}

func (n *fakeNode) url() string { return "ws" + strings.TrimPrefix(n.srv.URL, "http") + "/h2a/v1" }

func (n *fakeNode) config() Config { return Config{URL: n.url(), Token: goodToken} }

func (n *fakeNode) serve(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	n.headers = append(n.headers, r.Header.Clone())
	n.urls = append(n.urls, r.URL.String())
	n.mu.Unlock()
	if r.Header.Get("Origin") != "" {
		http.Error(w, "origin", http.StatusForbidden)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{Protocol}})
	if err != nil {
		return
	}
	if c.Subprotocol() != Protocol {
		c.Close(4400, "subprotocol")
		return
	}
	ctx := context.Background()
	_, b, err := c.Read(ctx)
	if err != nil {
		return
	}
	var h helloFrame
	json.Unmarshal(b, &h)
	n.mu.Lock()
	n.hellos = append(n.hellos, h)
	n.conns++
	var refuse func(*websocket.Conn) bool
	if len(n.refuse) > 0 {
		refuse, n.refuse = n.refuse[0], n.refuse[1:]
	}
	n.mu.Unlock()
	if refuse != nil && refuse(c) {
		return
	}
	if h.Token != goodToken {
		n.closeWith(c, CodeAuth, 4401)
		return
	}
	n.send(c, map[string]any{"type": "welcome", "v": "1", "project": "App",
		"to":     []Address{{Address: "lead", Name: "Project lead"}, {Address: "role:11", Name: "Editor"}},
		"limits": map[string]int{"text_bytes": n.textBytes, "ping_seconds": 20, "idle_seconds": 60}})
	n.mu.Lock()
	n.live = c
	var resend []Reply
	for _, m := range n.queue {
		if m.Seq > h.ResumeAfter {
			resend = append(resend, m)
		}
	}
	n.mu.Unlock()
	for _, m := range resend {
		n.sendReply(c, m)
	}
	for {
		_, b, err := c.Read(ctx)
		if err != nil {
			return
		}
		var f frame
		json.Unmarshal(b, &f)
		n.mu.Lock()
		n.frames = append(n.frames, f)
		hook := n.onMessage
		n.mu.Unlock()
		switch f.Type {
		case "ack":
			var a ackFrame
			json.Unmarshal(b, &a)
			n.mu.Lock()
			n.acked = max(n.acked, a.UpTo)
			q := n.queue[:0]
			for _, m := range n.queue {
				if m.Seq > a.UpTo {
					q = append(q, m)
				}
			}
			n.queue = q
			n.mu.Unlock()
		case "pong":
		case "message":
			var m messageFrame
			json.Unmarshal(b, &m)
			if hook != nil && hook(n, c, m) {
				continue
			}
			n.accept(c, m)
		}
	}
}

// accept delivers a message once per id and answers it.
func (n *fakeNode) accept(c *websocket.Conn, m messageFrame) {
	n.mu.Lock()
	if n.conv[m.ID] == "" {
		n.delivered[m.ID]++
		n.conv[m.ID] = "C" + strings.Repeat("x", 19)
	}
	conv := n.conv[m.ID]
	n.mu.Unlock()
	n.send(c, map[string]any{"type": "accepted", "re": m.ID, "conversation": conv})
}

// reply queues an agent's answer and sends it on the live connection.
func (n *fakeNode) reply(text string) Reply {
	n.mu.Lock()
	n.nextSeq++
	m := Reply{Seq: n.nextSeq, Conversation: "C" + strings.Repeat("x", 19), From: From{Project: "App", Role: "Editor"}, Text: text}
	n.queue = append(n.queue, m)
	c := n.live
	n.mu.Unlock()
	if c != nil {
		n.sendReply(c, m)
	}
	return m
}

func (n *fakeNode) sendReply(c *websocket.Conn, m Reply) {
	n.send(c, map[string]any{"type": "message", "seq": m.Seq, "conversation": m.Conversation,
		"from": m.From, "parts": []map[string]string{{"text": m.Text}}, "at": "2026-11-06T12:00:00Z"})
}

func (n *fakeNode) send(c *websocket.Conn, v any) {
	b, _ := json.Marshal(v)
	c.Write(context.Background(), websocket.MessageText, b)
}

// closeWith sends an error frame and closes with the code.
func (n *fakeNode) closeWith(c *websocket.Conn, code string, close int) {
	n.send(c, map[string]any{"type": "error", "code": code, "text": "the node says " + code})
	c.Close(websocket.StatusCode(close), code)
}

// drop ends the live connection without a frame (the network).
func (n *fakeNode) drop() {
	n.mu.Lock()
	c := n.live
	n.live = nil
	n.mu.Unlock()
	if c != nil {
		c.CloseNow()
	}
}

func (n *fakeNode) count() int { n.mu.Lock(); defer n.mu.Unlock(); return n.conns }

// waitFor polls cond for up to 5 s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out: %s", what)
}

// fastPauses makes the reconnect pauses short for a test.
func fastPauses(t *testing.T) {
	f, m, s := firstPause, maxPause, helloShutOut
	firstPause, maxPause, helloShutOut = 5*time.Millisecond, 40*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { firstPause, maxPause, helloShutOut = f, m, s })
}
