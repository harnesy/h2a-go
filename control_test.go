package h2a

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func controlConfig(n *fakeNode) Config {
	cfg := n.config()
	cfg.Can = []string{"state", "wake", "item"}
	return cfg
}

func ctx5(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (n *fakeNode) sentControls() []controlFrame {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]controlFrame(nil), n.controls...)
}

// A service of control signs in with text and its frames, gets the
// welcome's projects, asks the state of all or one project, and wakes and
// puts to sleep with the frames the contract has.
func TestControlFrames(t *testing.T) {
	n := newFakeNode(t)
	c := dial(t, n, controlConfig(n))
	n.mu.Lock()
	can := n.hellos[0].Can
	n.mu.Unlock()
	if strings.Join(can, ",") != "text,state,wake,item" {
		t.Fatalf("hello can %v", can)
	}
	w := c.Welcome()
	if len(w.Projects) != 1 || w.Projects[0].Project != "App" || len(w.Projects[0].Rights) != 4 || w.Projects[0].To[0].Address != "lead" {
		t.Fatalf("welcome projects %+v", w.Projects)
	}
	ctx := ctx5(t)
	s, err := c.State(ctx, "")
	if err != nil || !reflect.DeepEqual(s.Projects, fakeState) {
		t.Fatalf("state: %+v %v", s, err)
	}
	if _, err := c.State(ctx, "App"); err != nil {
		t.Fatal(err)
	}
	if err := c.Wake(ctx, "lead"); err != nil {
		t.Fatal(err)
	}
	if err := c.Sleep(ctx, "lead", ""); err != nil {
		t.Fatal(err)
	}
	if err := c.Sleep(ctx, "lead", SleepNow); err != nil {
		t.Fatal(err)
	}
	f := n.sentControls()
	if len(f) != 5 || f[0].Project != "" || f[1].Project != "App" || f[2].Type != "wake" || f[2].To != "lead" ||
		f[3].Type != "sleep" || f[3].Mode != "" || f[4].Mode != SleepNow {
		t.Fatalf("frames %+v", f)
	}
	for _, x := range f {
		if x.ID == "" {
			t.Fatalf("a frame without id: %+v", x)
		}
	}
}

// A service without control asks for nothing new: its hello is ["text"].
func TestNoControlNoProjects(t *testing.T) {
	n := newFakeNode(t)
	c := dial(t, n, n.config())
	if len(c.Welcome().Projects) != 0 {
		t.Fatalf("projects %+v", c.Welcome().Projects)
	}
}

// The node's state event reaches OnState.
func TestOnStateGetsAnEvent(t *testing.T) {
	n := newFakeNode(t)
	c := dial(t, n, controlConfig(n))
	got := make(chan State, 1)
	c.OnState(func(s State) { got <- s })
	ev := []ProjectState{{Project: "App", Posts: []Post{{Address: "lead", Name: "Project lead", State: PostUp, Counts: Counts{Seats: 1, Working: 1}}}}}
	n.event(ev)
	select {
	case s := <-got:
		if !reflect.DeepEqual(s.Projects, ev) {
			t.Fatalf("event %+v", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no event")
	}
}

// refuse answers the first frame of control with an error of code.
func refuseFirst(n *fakeNode, code string, extra map[string]any) *atomic.Int32 {
	var tries atomic.Int32
	n.onControl = func(n *fakeNode, c *websocket.Conn, m controlFrame) bool {
		if tries.Add(1) > 1 {
			return false
		}
		e := map[string]any{"type": "error", "re": m.ID, "code": code, "text": "the node says " + code}
		for k, v := range extra {
			e[k] = v
		}
		n.send(c, e)
		return true
	}
	return &tries
}

// A refused wake (h2a_no_wake) and sleep (h2a_not_yours) go to the caller as
// *Error; the connection stays and the next frame is answered.
func TestRefusedControlGoesToTheCaller(t *testing.T) {
	for _, code := range []string{CodeNoWake, CodeNotYours, CodeForbidden} {
		n := newFakeNode(t)
		refuseFirst(n, code, nil)
		c := dial(t, n, controlConfig(n))
		ctx := ctx5(t)
		do := c.Wake
		if code == CodeNotYours {
			do = func(ctx context.Context, to string) error { return c.Sleep(ctx, to, "") }
		}
		err := do(ctx, "lead")
		var e *Error
		if !errors.As(err, &e) || e.Code != code || e.Re == "" {
			t.Fatalf("%s: %v", code, err)
		}
		if err := c.Wake(ctx, "lead"); err != nil {
			t.Fatalf("%s: the next wake: %v", code, err)
		}
		if n.count() != 1 || len(n.sentControls()) != 2 {
			t.Fatalf("%s: %d connections, frames %+v", code, n.count(), n.sentControls())
		}
	}
}

// h2a_soft_stop is «wait until»: the same frame, the same id, once more
// after until; a ctx that ends before until gets the refusal at once.
func TestSoftStopWaitsUntil(t *testing.T) {
	n := newFakeNode(t)
	until := time.Now().Add(1500 * time.Millisecond).UTC().Truncate(time.Second)
	refuseFirst(n, CodeSoftStop, map[string]any{"until": until.Format(time.RFC3339)})
	c := dial(t, n, controlConfig(n))
	if err := c.Wake(ctx5(t), "lead"); err != nil {
		t.Fatal(err)
	}
	if time.Now().Before(until) {
		t.Fatalf("sent again before until %s", until)
	}
	f := n.sentControls()
	if len(f) != 2 || f[0].ID != f[1].ID {
		t.Fatalf("frames %+v", f)
	}

	n2 := newFakeNode(t)
	refuseFirst(n2, CodeSoftStop, map[string]any{"until": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	c2 := dial(t, n2, controlConfig(n2))
	start := time.Now()
	err := c2.Wake(ctx5(t), "lead")
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeSoftStop || e.Until.IsZero() || time.Since(start) > time.Second {
		t.Fatalf("a wait past ctx: %v after %s", err, time.Since(start))
	}
}

// h2a_rate on a wake is waited out (retry_after) and sent once more.
func TestWakeRateWaits(t *testing.T) {
	n := newFakeNode(t)
	tries := refuseFirst(n, CodeRate, map[string]any{"retry_after": 0})
	c := dial(t, n, controlConfig(n))
	if err := c.Wake(ctx5(t), "lead"); err != nil || tries.Load() != 2 {
		t.Fatalf("wake: %v after %d tries", err, tries.Load())
	}
}

// An item is accepted with its number; a text over the limit is refused
// here, without a frame.
func TestCreateItem(t *testing.T) {
	n := newFakeNode(t)
	c := dial(t, n, controlConfig(n))
	ctx := ctx5(t)
	ref, err := c.CreateItem(ctx, "role:11", "Scene 2", "Look at scene 2")
	if err != nil || ref.Item != 42 || ref.ID == "" {
		t.Fatalf("item: %+v %v", ref, err)
	}
	f := n.sentControls()
	if len(f) != 1 || f[0].Type != "item" || f[0].To != "role:11" || f[0].Title != "Scene 2" || f[0].Text != "Look at scene 2" {
		t.Fatalf("frames %+v", f)
	}
	_, err = c.CreateItem(ctx, "role:11", "Long", strings.Repeat("x", ItemTextBytes+1))
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeTooBig {
		t.Fatalf("a long text: %v", err)
	}
	if len(n.sentControls()) != 1 {
		t.Fatalf("a long text was sent: %+v", n.sentControls())
	}
}

// A wake waiting for its answer when the connection drops is sent again
// after the reconnect with the same id, and the node does it once.
func TestWakeResendKeepsID(t *testing.T) {
	fastPauses(t)
	n := newFakeNode(t)
	var dropped atomic.Bool
	n.onControl = func(n *fakeNode, c *websocket.Conn, m controlFrame) bool {
		if dropped.CompareAndSwap(false, true) {
			c.CloseNow() // gone before the accepted
			return true
		}
		return false
	}
	c := dial(t, n, controlConfig(n))
	if err := c.Wake(ctx5(t), "lead"); err != nil {
		t.Fatal(err)
	}
	f := n.sentControls()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(f) != 2 || f[0].ID != f[1].ID || n.done[f[0].ID] != 1 || n.conns != 2 {
		t.Fatalf("frames %+v, done %v over %d connections", f, n.done, n.conns)
	}
}
