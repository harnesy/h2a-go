// Package h2a is a client of harnsy's h2a v1 contract: an outside service
// holds one WebSocket to a harnsy node, gives tasks to a project's lead or a
// role, and gets the agents' answers, through disconnects.
//
// The contract is harnsy's (https://harnsy.dev/docs/h2a-v1); this package
// decides nothing the node decides: it follows the contract's «Client»
// column on every refusal and close, and passes the node's errors on as
// they are.
package h2a

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Protocol is the WebSocket subprotocol of the contract.
const Protocol = "harnsy.h2a.v1"

// Version is the contract version this SDK speaks.
const Version = "1"

// Address is one post a pass may write to: «lead» or «role:<id>».
type Address struct {
	Address string `json:"address"`
	Name    string `json:"name"`
	About   string `json:"about,omitempty"`
}

// Limits are the node's limits from the welcome (contract §6).
type Limits struct {
	FramesPerMinute int `json:"frames_per_minute"`
	MessagesPerDay  int `json:"messages_per_day"`
	TextBytes       int `json:"text_bytes"`
	FrameBytes      int `json:"frame_bytes"`
	Queue           int `json:"queue"`
	QueueHours      int `json:"queue_hours"`
	PingSeconds     int `json:"ping_seconds"`
	IdleSeconds     int `json:"idle_seconds"`
}

// Welcome is the node's answer to a good hello.
type Welcome struct {
	V       string    `json:"v"`
	Project string    `json:"project"`
	To      []Address `json:"to"`
	Limits  Limits    `json:"limits"`
}

// Message is a task or a reply the service sends. Without Conversation it
// starts a new conversation to To; with one it writes into it.
type Message struct {
	// ID is the service's id of the frame; empty: the SDK sets a unique one.
	// A repeated ID gets the same Accepted for 24 h and is not delivered twice.
	ID           string
	To           string
	Conversation string
	Text         string
}

// Accepted is the node's answer to a Message.
type Accepted struct {
	ID           string // the message's ID
	Conversation string
}

// From is who answered: never an agent's name, session or node.
type From struct {
	Project string `json:"project"`
	Role    string `json:"role"`
}

// Reply is an agent's answer.
type Reply struct {
	Seq          int64
	Conversation string
	From         From
	Text         string
	At           time.Time
}

// Error is a refusal of the node (contract §5), or the SDK's own refusal of
// a frame it knows the node would refuse (CodeTooBig).
type Error struct {
	Re         string // the refused frame's ID, "" for the connection
	Code       string
	Text       string // harnsy's words
	Field      string
	RetryAfter time.Duration
	Until      time.Time
	Close      int // the WebSocket close code that came with it, 0 none
}

func (e *Error) Error() string {
	s := "h2a: " + e.Code
	if e.Text != "" {
		s += ": " + e.Text
	}
	if e.Field != "" {
		s += " (field " + e.Field + ")"
	}
	return s
}

var (
	// ErrVersion: the node's welcome is not version 1.
	ErrVersion = errors.New("h2a: the node does not speak h2a version " + Version)
	// ErrReplaced: another copy of the service signed in with the same pass.
	ErrReplaced = errors.New("h2a: replaced: another connection of this pass signed in")
	// ErrClosed: the client was closed.
	ErrClosed = errors.New("h2a: closed")
)

// frame is any frame of the contract, as read.
type frame struct {
	Type         string          `json:"type"`
	V            string          `json:"v,omitempty"`
	Project      string          `json:"project,omitempty"`
	To           json.RawMessage `json:"to,omitempty"`
	Limits       *Limits         `json:"limits,omitempty"`
	Re           string          `json:"re,omitempty"`
	Conversation string          `json:"conversation,omitempty"`
	Seq          int64           `json:"seq,omitempty"`
	From         *From           `json:"from,omitempty"`
	Parts        []struct {
		Text *string `json:"text"`
	} `json:"parts,omitempty"`
	At         string `json:"at,omitempty"`
	Code       string `json:"code,omitempty"`
	Text       string `json:"text,omitempty"`
	Field      string `json:"field,omitempty"`
	RetryAfter int    `json:"retry_after,omitempty"`
	Until      string `json:"until,omitempty"`
}

func (f *frame) err() *Error {
	e := &Error{Re: f.Re, Code: f.Code, Text: f.Text, Field: f.Field, RetryAfter: time.Duration(f.RetryAfter) * time.Second}
	if f.Until != "" {
		e.Until, _ = time.Parse(time.RFC3339, f.Until)
	}
	return e
}

func (f *frame) text() string {
	for _, p := range f.Parts {
		if p.Text != nil {
			return *p.Text
		}
	}
	return ""
}

// Frames the service writes.
type helloFrame struct {
	Type        string   `json:"type"`
	V           string   `json:"v"`
	Token       string   `json:"token"`
	Name        string   `json:"name,omitempty"`
	Can         []string `json:"can"`
	ResumeAfter int64    `json:"resume_after,omitempty"`
}

// GoString keeps the token out of %#v.
func (h helloFrame) GoString() string { return fmt.Sprintf("hello{v:%q can:%v}", h.V, h.Can) }

type textPart struct {
	Text string `json:"text"`
}

type messageFrame struct {
	Type         string     `json:"type"`
	ID           string     `json:"id"`
	To           string     `json:"to,omitempty"`
	Conversation string     `json:"conversation,omitempty"`
	Parts        []textPart `json:"parts"`
}

type ackFrame struct {
	Type string `json:"type"`
	UpTo int64  `json:"up_to"`
}
