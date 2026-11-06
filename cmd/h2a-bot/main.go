// Command h2a-bot gives a task to a harnsy project's lead or role and prints
// the agents' answers, through disconnects.
//
//	H2A_URL=wss://node.example/h2a/v1 H2A_TOKEN=h2a_… h2a-bot -to lead "Check the release is built"
//
// Without -to it prints the pass's addresses and exits. The last handled
// seq is kept in ~/.h2a-bot.seq: what was not acknowledged comes at the next
// start. Ctrl+C exits without losing anything.
//
// Control (each needs the pass's right in the post's project):
//
//	h2a-bot -state                          the posts and their state
//	h2a-bot -wake lead                      wake the post's holder
//	h2a-bot -sleep lead                     put to sleep the holder this pass woke
//	h2a-bot -item "Scene 2" -text "Look"    raise an item; without -text, the text is read from stdin when it is piped
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	h2a "github.com/harnesy/h2a-go"
)

func main() {
	to := flag.String("to", "", "address: lead or role:<id> (none: list the addresses)")
	conv := flag.String("conv", "", "write into this conversation")
	listen := flag.Bool("listen", false, "send nothing, only print the answers")
	seqFile := flag.String("seq", filepath.Join(home(), ".h2a-bot.seq"), "where the last seq is kept")
	state := flag.Bool("state", false, "print the posts and their state (with -listen: and their changes)")
	wake := flag.String("wake", "", "wake the holder of this address")
	sleepTo := flag.String("sleep", "", "put to sleep the holder of this address that this pass woke")
	item := flag.String("item", "", "raise an item with this title (to -to, or lead)")
	itemText := flag.String("text", "", "the item's text (none: stdin, when it is piped)")
	flag.Parse()
	task := strings.Join(flag.Args(), " ")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	c, err := h2a.Dial(ctx, h2a.Config{
		URL:    os.Getenv("H2A_URL"),
		Token:  os.Getenv("H2A_TOKEN"),
		Can:    []string{"text", "state", "wake", "item"},
		Cursor: h2a.FileCursor(*seqFile),
		Logf:   log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	w := c.Welcome()

	if *state || *wake != "" || *sleepTo != "" || *item != "" {
		if err := control(ctx, c, *state, *wake, *sleepTo, *item, *itemText, *to); err != nil {
			log.Fatal(say(err))
		}
		if !*listen {
			return
		}
		if *state {
			c.OnState(func(s h2a.State) { printState("changed", s) })
		}
	}
	if *to == "" && *conv == "" && !*listen {
		fmt.Printf("project %s, addresses:\n", clean(w.Project))
		for _, a := range w.To {
			fmt.Printf("  %-12s %s\n", clean(a.Address), clean(a.Name))
		}
		for _, p := range w.Projects {
			fmt.Printf("project %s, rights %s:\n", clean(p.Project), clean(strings.Join(p.Rights, ", ")))
			for _, a := range p.To {
				fmt.Printf("  %-12s %s\n", clean(a.Address), clean(a.Name))
			}
		}
		return
	}
	if *to != "" && !known(w, *to) {
		log.Fatalf("%q is not an address of this pass: run without -to to list them", *to)
	}
	if task != "" && !*listen {
		acc, err := c.Send(ctx, h2a.Message{To: *to, Conversation: *conv, Text: task})
		if err != nil {
			log.Fatal(say(err))
		}
		fmt.Println("accepted, conversation", acc.Conversation)
	}

	err = c.Run(ctx, func(ctx context.Context, m h2a.Reply) error {
		fmt.Printf("[%s · %s] %s\n", clean(m.From.Role), clean(m.From.Project), clean(m.Text))
		return nil
	})
	switch {
	case errors.Is(err, context.Canceled):
	case errors.Is(err, h2a.ErrReplaced):
		log.Fatal("another copy of this service signed in with the same pass")
	case isCode(err, h2a.CodeAuth):
		log.Fatal("the pass is not valid any more (revoked or wrong): make a new one on the dashboard")
	case isCode(err, h2a.CodeOff):
		log.Fatal("access from outside or this pass is turned off on the node")
	case err != nil:
		log.Fatal(err)
	}
}

// known says address is one of the welcome's.
func known(w h2a.Welcome, address string) bool {
	for _, a := range w.To {
		if a.Address == address {
			return true
		}
	}
	for _, p := range w.Projects {
		for _, a := range p.To {
			if a.Address == address {
				return true
			}
		}
	}
	return false
}

// control does the -state, -wake, -sleep and -item asked, in that order, and
// prints what the node answered.
func control(ctx context.Context, c *h2a.Client, state bool, wake, sleepTo, item, text, to string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute) // a longer wait (rate, soft stop) is said, not waited
	defer cancel()
	if state {
		s, err := c.State(ctx, "")
		if err != nil {
			return err
		}
		printState("now", s)
	}
	if wake != "" {
		if err := c.Wake(ctx, wake); err != nil {
			return err
		}
		fmt.Println("woken (or awake already):", clean(wake))
	}
	if sleepTo != "" {
		if err := c.Sleep(ctx, sleepTo, ""); err != nil {
			return err
		}
		fmt.Println("goes to sleep after its turn:", clean(sleepTo))
	}
	if item != "" {
		if to == "" {
			to = "lead"
		}
		if text == "" {
			if st, err := os.Stdin.Stat(); err == nil && st.Mode()&os.ModeCharDevice == 0 {
				b, err := io.ReadAll(io.LimitReader(os.Stdin, h2a.ItemTextBytes+1))
				if err != nil {
					return err
				}
				text = string(b)
			}
		}
		ref, err := c.CreateItem(ctx, to, item, text)
		if err != nil {
			return err
		}
		fmt.Printf("item #%d raised for %s\n", ref.Item, clean(to))
	}
	return nil
}

// stateWords are the posts' state words in plain words.
var stateWords = map[string]string{
	h2a.PostVacant: "nobody on the post", h2a.PostStarting: "starting", h2a.PostUp: "awake",
	h2a.PostPartlyAsleep: "partly asleep", h2a.PostAsleep: "asleep", h2a.PostOff: "off", h2a.PostArchived: "archived",
}

func printState(when string, s h2a.State) {
	for _, p := range s.Projects {
		fmt.Printf("project %s (%s):\n", clean(p.Project), when)
		for _, x := range p.Posts {
			word, ok := stateWords[x.State]
			if !ok {
				word = clean(x.State)
			}
			n := x.Counts
			fmt.Printf("  %-12s %-20s %s — %d seat(s): %d working, %d waiting, %d idle, %d asleep, %d off\n",
				clean(x.Address), clean(x.Name), word, n.Seats, n.Working, n.Waiting, n.Idle, n.Asleep, n.Off)
		}
	}
}

// codeWords are the node's refusals in plain words.
var codeWords = map[string]string{
	h2a.CodeForbidden: "this pass has no right to that in the post's project (the person gives it on the dashboard)",
	h2a.CodeNoWake:    "a service may not wake this holder: it sleeps by a person's or a lead's choice, is off, or asked not to be woken",
	h2a.CodeNotYours:  "this pass did not wake that holder last, so it may not put it to sleep",
	h2a.CodeSoftStop:  "the subscription is at its soft stop: harnsy wakes nobody",
	h2a.CodeRate:      "too many of these for now",
	h2a.CodeTooBig:    "the text is too long",
	h2a.CodeNoHolder:  "nobody holds that post now",
	h2a.CodeAddress:   "that is not an address of this pass",
	h2a.CodeFrame:     "the node does not take that frame from this pass",
	h2a.CodeDaily:     "the day's messages are used up",
}

// say is err in plain words when it is a refusal the bot knows.
func say(err error) string {
	var e *h2a.Error
	if !errors.As(err, &e) {
		return err.Error()
	}
	w, ok := codeWords[e.Code]
	if !ok {
		return clean(err.Error())
	}
	switch {
	case e.Code == h2a.CodeRate && e.RetryAfter > 0:
		w += ": try again in " + e.RetryAfter.String()
	case !e.Until.IsZero():
		w += " until " + e.Until.Local().Format(time.DateTime)
	}
	return w + " (" + e.Code + ")"
}

func home() string {
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return "."
}

// clean drops control characters (escape sequences, bidi marks) from an
// agent's text before the terminal shows it; line breaks and tabs stay.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, s)
}

// isCode says err is the node's refusal with that code.
func isCode(err error, code string) bool {
	var e *h2a.Error
	return errors.As(err, &e) && e.Code == code
}
