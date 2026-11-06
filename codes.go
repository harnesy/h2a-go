package h2a

import (
	"math/rand/v2"
	"time"
)

// The codes of the contract's §5 (testdata/codes.json, from harnsy).
const (
	CodeVersion             = "h2a_version"
	CodeHelloRequired       = "h2a_hello_required"
	CodeAuth                = "h2a_auth"
	CodeOff                 = "h2a_off"
	CodeReplaced            = "h2a_replaced"
	CodePing                = "h2a_ping"
	CodeRate                = "h2a_rate"
	CodeBadFrame            = "h2a_bad_frame"
	CodeFrame               = "h2a_frame"
	CodePart                = "h2a_part"
	CodeTooBig              = "h2a_too_big"
	CodeAddress             = "h2a_address"
	CodeConversationUnknown = "h2a_conversation_unknown"
	CodeNoHolder            = "h2a_no_holder"
	CodeDaily               = "h2a_daily"
	CodeForbidden           = "h2a_forbidden"
	CodeNotYours            = "h2a_not_yours"
	CodeNoWake              = "h2a_no_wake"
	CodeSoftStop            = "h2a_soft_stop"
)

// What a service does (the contract's «Client» column).
const (
	MoveReconnect = "reconnect"
	MoveStop      = "stop"
	MoveWait      = "wait retry_after"
	MoveCaller    = "caller"
	MoveWaitUntil = "wait until"
)

// Where a code comes.
const (
	onHello = "hello"
	onFrame = "frame"
	onClose = "close"
)

// rule is one row of the contract's «Client» column.
type rule struct {
	Code   string `json:"code,omitempty"`
	On     string `json:"on"`
	Close  int    `json:"close,omitempty"`
	Client string `json:"client"`
}

// rules are the contract's rows, as harnsy's codes.json has them (the test
// holds them to the copy in testdata).
var rules = []rule{
	{CodeVersion, onHello, 4400, MoveStop},
	{CodeHelloRequired, onHello, 4400, MoveStop},
	{CodeAuth, onHello, 4401, MoveStop},
	{CodeOff, onHello, 4403, MoveStop},
	{CodeReplaced, onClose, 4409, MoveStop},
	{CodePing, onClose, 4408, MoveReconnect},
	{CodeRate, onHello, 4429, MoveWait},
	{CodeBadFrame, onHello, 4400, MoveStop},
	{CodeFrame, onFrame, 0, MoveCaller},
	{CodeBadFrame, onFrame, 0, MoveCaller},
	{CodePart, onFrame, 0, MoveCaller},
	{CodeTooBig, onFrame, 0, MoveCaller},
	{CodeAddress, onFrame, 0, MoveCaller},
	{CodeConversationUnknown, onFrame, 0, MoveCaller},
	{CodeNoHolder, onFrame, 0, MoveCaller},
	{CodeRate, onFrame, 0, MoveWait},
	{CodeDaily, onFrame, 0, MoveCaller},
	{CodeForbidden, onFrame, 0, MoveCaller},
	{CodeNotYours, onFrame, 0, MoveCaller},
	{CodeNoWake, onFrame, 0, MoveCaller},
	{CodeSoftStop, onFrame, 0, MoveWaitUntil},
	{"", onClose, 1006, MoveReconnect},
	{"", onClose, 1009, MoveReconnect},
}

// closeMove is what to do when the connection ended with the close code and,
// when one came just before, the error frame's code. An unknown close is a
// reconnect, as the network's.
func closeMove(code string, closeCode int) string {
	for _, r := range rules {
		if r.On != onFrame && r.Close == closeCode && closeCode != 0 && (r.Code == "" || code == "" || r.Code == code) {
			return r.Client
		}
	}
	for _, r := range rules {
		if r.On != onFrame && code != "" && r.Code == code {
			return r.Client
		}
	}
	return MoveReconnect
}

// frameMove is what to do with a refused frame on a live connection.
func frameMove(code string) string {
	for _, r := range rules {
		if r.On == onFrame && r.Code == code {
			return r.Client
		}
	}
	for _, r := range rules {
		if r.On != onFrame && r.Code == code {
			return r.Client // a connection's code that came without its close
		}
	}
	return MoveCaller
}

// backoff is the contract's pause before a reconnect: 1 s, ×2 to 60 s,
// ±20 % at random.
type backoff struct {
	First, Max time.Duration
	Factor     float64
	Jitter     float64
	next       time.Duration
}

// firstPause and maxPause are the contract's 1 s and 60 s (variables for
// the package's tests).
var firstPause, maxPause = time.Second, time.Minute

func newBackoff() *backoff { return &backoff{First: firstPause, Max: maxPause, Factor: 2, Jitter: 0.2} }

// pause is the next pause, and grows the one after it.
func (b *backoff) pause() time.Duration {
	if b.next == 0 {
		b.next = b.First
	}
	d := b.next
	b.next = min(time.Duration(float64(b.next)*b.Factor), b.Max)
	return time.Duration(float64(d) * (1 - b.Jitter + 2*b.Jitter*rand.Float64()))
}

// reset starts again from the first pause (after a welcome).
func (b *backoff) reset() { b.next = 0 }

// helloShutOut is the wait after h2a_rate on a hello without retry_after.
var helloShutOut = 10 * time.Minute
