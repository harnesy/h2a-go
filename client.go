package h2a

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Config is how to reach the node and sign in.
type Config struct {
	// URL is the gateway's address of the contract: wss://<host>[:port]/h2a/v1.
	URL string
	// Token is the pass's token (h2a_…): a secret. It goes in the hello
	// frame only, never in the URL, a header or a log of this package.
	Token string
	// Can is what the service takes and sends: "text" (always sent, also
	// when not listed), and by need "state" (State, OnState), "wake" (Wake
	// and Sleep), "item" (CreateItem). nil: ["text"].
	Can []string
	// Cursor keeps the last handled seq; nil: MemCursor.
	Cursor Cursor
	// HTTPClient dials the WebSocket; nil: http.DefaultClient.
	HTTPClient *http.Client
	// Logf, when set, gets a line on each connect and reconnect (never the
	// token).
	Logf func(format string, args ...any)
}

// Client is one service's connection to the node: Dial signs in, Send gives
// tasks, Run takes the answers; the connection is kept up by the client,
// as the contract's «Client» column says.
type Client struct {
	cfg Config

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when the client stops for good

	mu        sync.Mutex
	conn      *websocket.Conn
	live      chan struct{} // closed while conn is signed in
	welcome   Welcome
	pending   map[string]*pending
	stopErr   error
	delivered int64 // the highest seq put on replies

	replies chan Reply
	states  chan State // the node's state events, for the OnState handler
	onState func(State)
	errs    chan *Error
	redo    *Reply // a reply whose handler failed: the next Run gets it first
	running atomic.Bool
}

type pending struct {
	frame []byte
	res   chan error // nil: answered
	ans   frame      // the node's answer: accepted, or state with re
}

// readTimeout: no frame for this long (the node pings every 20 s) is a dead
// connection.
const readTimeout = 75 * time.Second

// helloTimeout: the welcome must come within this.
const helloTimeout = 15 * time.Second

// Dial connects and signs in. A refused hello comes back as *Error (its
// Code says why: CodeAuth, CodeOff, CodeRate…), a welcome of another
// version as ErrVersion. The client then keeps the connection up until ctx
// ends, Close is called, or the contract says to stop.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.URL == "" || cfg.Token == "" {
		return nil, errors.New("h2a: Config needs URL and Token")
	}
	if err := checkURL(cfg.URL); err != nil {
		return nil, err
	}
	cfg.Can = withText(cfg.Can)
	if cfg.Cursor == nil {
		cfg.Cursor = MemCursor()
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	c := &Client{cfg: cfg, done: make(chan struct{}), live: make(chan struct{}),
		pending: map[string]*pending{}, replies: make(chan Reply, 64), states: make(chan State, 16), errs: make(chan *Error, 16)}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	conn, w, err := c.connect(ctx)
	if err != nil {
		c.cancel()
		return nil, err
	}
	c.signedIn(conn, w)
	go c.loop(conn)
	go c.dispatchStates()
	return c, nil
}

// withText is can with "text" first when it lacks it: the node refuses a
// hello without it.
func withText(can []string) []string {
	for _, v := range can {
		if v == "text" {
			return can
		}
	}
	return append([]string{"text"}, can...)
}

// Welcome is the node's welcome of the current connection.
func (c *Client) Welcome() Welcome {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.welcome
}

// Close ends the connection; Send and Run return ErrClosed.
func (c *Client) Close() error {
	c.cancel()
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		conn.Close(websocket.StatusNormalClosure, "")
	}
	<-c.done
	return nil
}

// Send gives a task or a reply to the node and waits for its Accepted. A
// disconnect before it is resent with the same ID after the reconnect (the
// node answers a repeated ID with the same Accepted and delivers it once);
// h2a_rate is waited out once. A refusal comes back as *Error.
func (c *Client) Send(ctx context.Context, m Message) (Accepted, error) {
	if m.ID == "" {
		m.ID = newID()
	}
	if lim := c.Welcome().Limits.TextBytes; lim > 0 && len(m.Text) > lim {
		return Accepted{}, &Error{Re: m.ID, Code: CodeTooBig, Text: fmt.Sprintf("text of %d bytes, the node takes %d", len(m.Text), lim)}
	}
	b, _ := json.Marshal(messageFrame{Type: "message", ID: m.ID, To: m.To, Conversation: m.Conversation, Parts: []textPart{{m.Text}}})
	f, err := c.request(ctx, m.ID, b)
	if err != nil {
		return Accepted{}, err
	}
	return Accepted{ID: f.Re, Conversation: f.Conversation}, nil
}

// request sends a frame with its id and waits for the node's answer with
// that re. A refusal is *Error, done as the contract's «Client» column
// says: «wait retry_after» (h2a_rate) and «wait until» (h2a_soft_stop) send
// the same frame, with the same id, once more after the wait; a wait that
// would outlast ctx returns the refusal at once. «caller» returns it.
func (c *Client) request(ctx context.Context, id string, b []byte) (frame, error) {
	for try := 0; ; try++ {
		f, err := c.sendOnce(ctx, id, b)
		var e *Error
		if try > 0 || !errors.As(err, &e) {
			return f, err
		}
		var wait time.Duration
		switch frameMove(e.Code) {
		case MoveWait:
			wait = e.RetryAfter
		case MoveWaitUntil:
			if e.Until.IsZero() {
				return f, err
			}
			wait = time.Until(e.Until)
		default:
			return f, err
		}
		if dl, ok := ctx.Deadline(); ok && time.Now().Add(wait).After(dl) {
			return f, err
		}
		if err := sleep(ctx, c.done, max(wait, 0)); err != nil {
			return frame{}, err
		}
	}
}

func (c *Client) sendOnce(ctx context.Context, id string, b []byte) (frame, error) {
	p := &pending{frame: b, res: make(chan error, 1)}
	c.mu.Lock()
	if c.stopErr != nil {
		err := c.stopErr
		c.mu.Unlock()
		return frame{}, err
	}
	c.pending[id] = p
	conn, live := c.conn, c.live
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	select {
	case <-live:
		conn.Write(ctx, websocket.MessageText, b) // a failed write: the reconnect resends it
	default: // not signed in now: the reconnect sends it
	}
	select {
	case err := <-p.res:
		return p.ans, err
	case <-ctx.Done():
		return frame{}, ctx.Err()
	case <-c.done:
		return frame{}, c.stopped()
	}
}

// ErrRunning: Run was called while another Run of the client runs.
var ErrRunning = errors.New("h2a: Run is already running on this client")

// checkURL takes only wss://host[:port]/path (ws:// only to this machine,
// for tests): no user or password and no query in it — the pass goes in the
// hello alone, and a URL lands in logs.
func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("h2a: bad URL: %w", err)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("h2a: the URL takes no user, password, query or fragment: the token goes in the hello")
	}
	switch u.Scheme {
	case "wss":
		return nil
	case "ws":
		if ip := net.ParseIP(u.Hostname()); u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback()) {
			return nil
		}
	}
	return errors.New("h2a: the URL must be wss:// (ws:// only to this machine)")
}

// Run hands each agent's reply to h, in seq order, one at a time. h
// returning nil acknowledges the reply (ack) and saves its seq in the
// Cursor; an error is returned by Run, and the next Run gets that reply
// again. A refusal of the connection without a frame's ID comes back as
// *Error. Run returns when ctx ends, or the client stops for good (the
// contract's «stop»: *Error with CodeAuth or CodeOff, ErrReplaced). One Run
// at a time per client (a second gets ErrRunning). Keep h short: while it
// runs, no new reply is read past the buffer, and a long stall loses the
// connection (the SDK reconnects and nothing is lost, but it costs time).
func (c *Client) Run(ctx context.Context, h func(ctx context.Context, m Reply) error) error {
	if !c.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer c.running.Store(false)
	for {
		var m Reply
		if c.redo != nil {
			m, c.redo = *c.redo, nil
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.done:
				return c.stopped()
			case e := <-c.errs:
				return e
			case m = <-c.replies:
			}
		}
		if err := h(ctx, m); err != nil {
			c.redo = &m
			return err
		}
		if err := c.cfg.Cursor.Save(m.Seq); err != nil {
			return fmt.Errorf("h2a: save the cursor: %w", err)
		}
		c.write(ackFrame{Type: "ack", UpTo: m.Seq})
	}
}

// stopped is why the client stopped.
func (c *Client) stopped() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopErr != nil {
		return c.stopErr
	}
	return ErrClosed
}

// write sends a frame on the live connection, if any (an ack lost to a
// disconnect is sent again after the next welcome, from the Cursor).
func (c *Client) write(v any) {
	b, _ := json.Marshal(v)
	c.mu.Lock()
	conn, live := c.conn, c.live
	c.mu.Unlock()
	select {
	case <-live:
		conn.Write(c.ctx, websocket.MessageText, b)
	default:
	}
}

// connect dials, sends the hello and reads the welcome.
func (c *Client) connect(ctx context.Context) (*websocket.Conn, Welcome, error) {
	conn, _, err := websocket.Dial(ctx, c.cfg.URL, &websocket.DialOptions{Subprotocols: []string{Protocol}, HTTPClient: c.cfg.HTTPClient})
	if err != nil {
		return nil, Welcome{}, fmt.Errorf("h2a: connect: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	cur, err := c.cfg.Cursor.Load()
	if err != nil {
		conn.CloseNow()
		return nil, Welcome{}, fmt.Errorf("h2a: load the cursor: %w", err)
	}
	hello, _ := json.Marshal(helloFrame{Type: "hello", V: Version, Token: c.cfg.Token, Can: c.cfg.Can, ResumeAfter: cur})
	if err := conn.Write(ctx, websocket.MessageText, hello); err != nil {
		conn.CloseNow()
		return nil, Welcome{}, errors.New("h2a: the hello was not sent: the connection closed")
	}
	rctx, cancel := context.WithTimeout(ctx, helloTimeout)
	defer cancel()
	_, b, err := conn.Read(rctx)
	if err != nil {
		conn.CloseNow()
		return nil, Welcome{}, &Error{Code: "", Text: "no welcome", Close: int(websocket.CloseStatus(err))}
	}
	var f frame
	if json.Unmarshal(b, &f) != nil {
		conn.CloseNow()
		return nil, Welcome{}, errors.New("h2a: the node's answer to the hello is not JSON")
	}
	switch f.Type {
	case "error":
		e := f.err()
		_, _, rerr := conn.Read(rctx) // the close that follows
		e.Close = int(websocket.CloseStatus(rerr))
		conn.CloseNow()
		return nil, Welcome{}, e
	case "welcome":
	default:
		conn.CloseNow()
		return nil, Welcome{}, fmt.Errorf("h2a: the node answered the hello with %q", f.Type)
	}
	if f.V != Version {
		conn.Close(websocket.StatusNormalClosure, "")
		return nil, Welcome{}, ErrVersion
	}
	w := Welcome{V: f.V, Project: f.Project}
	json.Unmarshal(f.To, &w.To)
	json.Unmarshal(f.Projects, &w.Projects)
	if f.Limits != nil {
		w.Limits = *f.Limits
	}
	if cur > 0 {
		ack, _ := json.Marshal(ackFrame{Type: "ack", UpTo: cur})
		conn.Write(ctx, websocket.MessageText, ack)
	}
	return conn, w, nil
}

// signedIn makes conn the live connection and sends what waits on it.
func (c *Client) signedIn(conn *websocket.Conn, w Welcome) {
	c.mu.Lock()
	c.conn, c.welcome = conn, w
	close(c.live)
	var frames [][]byte
	for _, p := range c.pending {
		frames = append(frames, p.frame)
	}
	if cur, err := c.cfg.Cursor.Load(); err == nil && cur > c.delivered {
		c.delivered = cur
	}
	c.mu.Unlock()
	for _, b := range frames {
		conn.Write(c.ctx, websocket.MessageText, b)
	}
}

// lost marks the connection gone.
func (c *Client) lost() {
	c.mu.Lock()
	c.conn = nil
	c.live = make(chan struct{})
	c.mu.Unlock()
}

// stop ends the client for good with err.
func (c *Client) stop(err error) {
	c.mu.Lock()
	if c.stopErr == nil {
		c.stopErr = err
	}
	c.conn = nil
	c.mu.Unlock()
	close(c.done)
}

// loop reads the live connection and, when it ends, does what the contract
// says: reconnect after the pause, wait retry_after, or stop.
func (c *Client) loop(conn *websocket.Conn) {
	b := newBackoff()
	for {
		code, closeCode := c.read(conn)
		c.lost()
		if c.ctx.Err() != nil {
			c.stop(ErrClosed)
			return
		}
		var wait time.Duration
		switch closeMove(code, closeCode) {
		case MoveStop:
			c.stop(stopError(code, closeCode))
			return
		case MoveWait:
			wait = helloShutOut
		default:
			wait = b.pause()
		}
		for {
			c.cfg.Logf("h2a: reconnecting in %s (%s, close %d)", wait.Round(time.Millisecond), orNetwork(code), closeCode)
			if sleep(c.ctx, nil, wait) != nil {
				c.stop(ErrClosed)
				return
			}
			next, w, err := c.connect(c.ctx)
			if err == nil {
				b.reset()
				c.cfg.Logf("h2a: signed in again")
				c.signedIn(next, w)
				conn = next
				break
			}
			var e *Error
			switch {
			case errors.Is(err, ErrVersion):
				c.stop(err)
				return
			case errors.As(err, &e) && e.Code != "":
				switch closeMove(e.Code, e.Close) {
				case MoveStop:
					c.stop(e)
					return
				case MoveWait:
					wait = e.RetryAfter
					if wait <= 0 {
						wait = helloShutOut
					}
					continue
				}
			}
			wait = b.pause()
		}
	}
}

// read reads conn until it ends: the code of the error frame that came last
// without an ID (a close's reason), and the close code.
func (c *Client) read(conn *websocket.Conn) (code string, closeCode int) {
	for {
		ctx, cancel := context.WithTimeout(c.ctx, readTimeout)
		_, b, err := conn.Read(ctx)
		cancel()
		if err != nil {
			conn.CloseNow()
			return code, int(websocket.CloseStatus(err))
		}
		var f frame
		if json.Unmarshal(b, &f) != nil {
			continue
		}
		switch f.Type {
		case "ping":
			conn.Write(c.ctx, websocket.MessageText, []byte(`{"type":"pong"}`))
		case "accepted":
			c.answer(f.Re, f, nil)
		case "state":
			if f.Re != "" {
				c.answer(f.Re, f, nil)
				continue
			}
			c.stateEvent(f)
		case "error":
			e := f.err()
			if f.Re != "" {
				c.answer(f.Re, frame{}, e)
				continue
			}
			code = f.Code
			if frameMove(f.Code) == MoveCaller {
				select {
				case c.errs <- e:
				default:
				}
			}
		case "message":
			c.reply(f)
		}
	}
}

// answer ends a waiting request.
func (c *Client) answer(id string, f frame, err error) {
	c.mu.Lock()
	p := c.pending[id]
	c.mu.Unlock()
	if p == nil {
		return
	}
	p.ans = f
	select {
	case p.res <- err:
	default:
	}
}

// reply puts an agent's reply on the queue, once: a seq already put there
// (sent again after a reconnect) is skipped.
func (c *Client) reply(f frame) {
	c.mu.Lock()
	if f.Seq <= c.delivered {
		c.mu.Unlock()
		return
	}
	c.delivered = f.Seq
	c.mu.Unlock()
	m := Reply{Seq: f.Seq, Conversation: f.Conversation, Text: f.text()}
	if f.From != nil {
		m.From = *f.From
	}
	m.At, _ = time.Parse(time.RFC3339, f.At)
	select {
	case c.replies <- m:
	case <-c.ctx.Done():
	}
}

// stopError is the error a stop leaves for Send and Run.
func stopError(code string, closeCode int) error {
	if code == CodeReplaced || closeCode == 4409 {
		return ErrReplaced
	}
	if code == "" {
		code = fmt.Sprintf("close %d", closeCode)
	}
	return &Error{Code: code, Close: closeCode}
}

func orNetwork(code string) string {
	if code == "" {
		return "the connection dropped"
	}
	return code
}

// sleep waits d, or until ctx ends or done closes.
func sleep(ctx context.Context, done chan struct{}, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return ErrClosed
	}
}

// newID is a unique message ID: [A-Za-z0-9._:-], 27 characters.
func newID() string {
	var b [12]byte
	rand.Read(b[:])
	return "go-" + hex.EncodeToString(b[:])
}
