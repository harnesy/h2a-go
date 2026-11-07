package h2a

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
)

// The frames of control (contract §2): the posts' state, wake and sleep,
// an item. Each needs its word in Config.Can ("state", "wake" — sleep rides
// on it —, "item") and the pass's right in the address's project (read,
// wake, items); the node decides both and refuses with h2a_frame or
// h2a_forbidden.

// State is the posts of the pass's projects and their state: the answer to
// a request, or an event (only the posts that changed).
type State struct {
	Projects []ProjectState `json:"projects"`
}

// ProjectState is one project's posts.
type ProjectState struct {
	Project string `json:"project"`
	Posts   []Post `json:"posts"`
}

// Post is one post (an address) and its state: harnsy's team-state words
// for the role, word for word. Never a seat, an agent, a session or a node.
type Post struct {
	Address string `json:"address"`
	Name    string `json:"name"`
	State   string `json:"state"` // PostVacant, PostStarting, PostUp, …
	Counts  Counts `json:"counts"`
}

// Counts are the role's seats by what they do now.
type Counts struct {
	Seats    int `json:"seats"`
	Working  int `json:"working"`
	Waiting  int `json:"waiting"`
	Starting int `json:"starting"`
	Idle     int `json:"idle"`
	Asleep   int `json:"asleep"`
	Off      int `json:"off"`
	Vacant   int `json:"vacant"`
}

// The words of Post.State (harnsy's team-state v1). A word not here is
// passed on as it is.
const (
	PostVacant       = "vacant" // nobody holds the post
	PostStarting     = "starting"
	PostUp           = "up"
	PostPartlyAsleep = "partly_asleep"
	PostAsleep       = "asleep"
	PostOff          = "off"
	PostArchived     = "archived"
)

// ItemRef is the node's answer to an item: its number.
type ItemRef struct {
	ID   string // the frame's id
	Item int64  // the item's number on the node
}

// ItemTextBytes is the contract's limit of an item's text: the text with
// harnsy's frame around it (§6). The frame's words are the node's, so the
// SDK refuses here only a text that is over the limit by itself; one under
// it may still be refused by the node with h2a_too_big.
const ItemTextBytes = 8000

// SleepAfterStep is the sleep mode: after the holder's turn, the node's
// default and the only one it keeps (it takes "now" as after_step too).
const SleepAfterStep = "after_step"

type stateFrame struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Project string `json:"project,omitempty"`
}

type wakeFrame struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	To   string `json:"to"`
	Mode string `json:"mode,omitempty"`
}

type itemFrame struct {
	Type  string `json:"type"`
	ID    string `json:"id"`
	To    string `json:"to"`
	Title string `json:"title"`
	Text  string `json:"text,omitempty"`
}

// State asks the posts and their state of project (its name, as
// Welcome().Projects has it), or of every project where the pass has read
// when project is "". Needs "state" in Config.Can.
func (c *Client) State(ctx context.Context, project string) (State, error) {
	id := newID()
	b, _ := json.Marshal(stateFrame{Type: "state", ID: id, Project: project})
	f, err := c.request(ctx, id, b)
	if err != nil {
		return State{}, err
	}
	return stateOf(f)
}

// stateOf reads a node's state frame.
func stateOf(f frame) (State, error) {
	var s State
	if err := json.Unmarshal(f.Projects, &s.Projects); err != nil {
		return State{}, fmt.Errorf("h2a: the node's state frame: %w", err)
	}
	return s, nil
}

// OnState sets h to get the node's state events: the posts whose state or
// counts changed (at most once per 5 s a post), of projects where the pass
// has read. h runs on one goroutine of the client, one event at a time.
// The events that come while h runs are coalesced: its next call has each
// post that changed once, in its newest state; the client never waits on h
// to read the node. nil: events are dropped. Needs "state" in Config.Can.
func (c *Client) OnState(h func(State)) {
	c.mu.Lock()
	c.onState = h
	c.mu.Unlock()
}

// Wake wakes the holder of the post to (an address of Welcome().Projects).
// The node wakes only a holder this pass or harnsy put to sleep: else
// h2a_no_wake. A holder awake already: nil. Needs "wake" in Config.Can.
func (c *Client) Wake(ctx context.Context, to string) error {
	id := newID()
	b, _ := json.Marshal(wakeFrame{Type: "wake", ID: id, To: to})
	_, err := c.request(ctx, id, b)
	return err
}

// Sleep puts to sleep the holder of to that this pass woke last (else
// h2a_not_yours), after its turn. mode "" is SleepAfterStep (the node's
// default, not sent). Needs "wake" in Config.Can.
func (c *Client) Sleep(ctx context.Context, to, mode string) error {
	id := newID()
	b, _ := json.Marshal(wakeFrame{Type: "sleep", ID: id, To: to, Mode: mode})
	_, err := c.request(ctx, id, b)
	return err
}

// CreateItem raises a new item of the post's role. The node writes its
// title as its own ([service «…»] and title, one line, 120 characters) and
// frames text as data from outside. A text over ItemTextBytes is refused
// here (h2a_too_big), without a frame. Needs "item" in Config.Can.
func (c *Client) CreateItem(ctx context.Context, to, title, text string) (ItemRef, error) {
	id := newID()
	if len(text) > ItemTextBytes {
		return ItemRef{}, &Error{Re: id, Code: CodeTooBig, Text: fmt.Sprintf("an item's text of %d bytes, the node takes %d with its frame", len(text), ItemTextBytes)}
	}
	b, _ := json.Marshal(itemFrame{Type: "item", ID: id, To: to, Title: title, Text: text})
	f, err := c.request(ctx, id, b)
	if err != nil {
		return ItemRef{}, err
	}
	return ItemRef{ID: f.Re, Item: f.Item}, nil
}

// stateQueue holds the state events the OnState handler has not got yet,
// coalesced: one entry per post, its newest state. The read loop never
// waits on it, and a slow handler never misses the newest state of a post.
type stateQueue struct {
	mu       sync.Mutex
	projects []ProjectState
	ready    chan struct{} // a value while projects is not empty
}

// put merges s into the queue: a post already queued takes its new state
// in place.
func (q *stateQueue) put(s State) {
	q.mu.Lock()
	for _, p := range s.Projects {
		i := slices.IndexFunc(q.projects, func(x ProjectState) bool { return x.Project == p.Project })
		if i < 0 {
			q.projects = append(q.projects, ProjectState{Project: p.Project})
			i = len(q.projects) - 1
		}
		posts := q.projects[i].Posts
		for _, post := range p.Posts {
			if j := slices.IndexFunc(posts, func(x Post) bool { return x.Address == post.Address }); j >= 0 {
				posts[j] = post
			} else {
				posts = append(posts, post)
			}
		}
		q.projects[i].Posts = posts
	}
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// take empties the queue: one State with every post queued.
func (q *stateQueue) take() State {
	q.mu.Lock()
	defer q.mu.Unlock()
	s := State{Projects: q.projects}
	q.projects = nil
	return s
}

// stateEvent queues a state event for the handler, without waiting.
func (c *Client) stateEvent(f frame) {
	s, err := stateOf(f)
	if err != nil {
		return
	}
	c.states.put(s)
}

// dispatchStates hands the events to the OnState handler until the client
// stops.
func (c *Client) dispatchStates() {
	for {
		select {
		case <-c.done:
			return
		case <-c.states.ready:
			s := c.states.take()
			if len(s.Projects) == 0 {
				continue
			}
			c.mu.Lock()
			h := c.onState
			c.mu.Unlock()
			if h != nil {
				h(s)
			}
		}
	}
}
