// Command h2a-bot gives a task to a harnsy project's lead or role and prints
// the agents' answers, through disconnects.
//
//	H2A_URL=wss://node.example/h2a/v1 H2A_TOKEN=h2a_… h2a-bot -to lead "Check the release is built"
//
// Without -to it prints the pass's addresses and exits. The last handled
// seq is kept in ~/.h2a-bot.seq: what was not acknowledged comes at the next
// start. Ctrl+C exits without losing anything.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"unicode"

	h2a "github.com/harnesy/h2a-go"
)

func main() {
	to := flag.String("to", "", "address: lead or role:<id> (none: list the addresses)")
	conv := flag.String("conv", "", "write into this conversation")
	listen := flag.Bool("listen", false, "send nothing, only print the answers")
	seqFile := flag.String("seq", filepath.Join(home(), ".h2a-bot.seq"), "where the last seq is kept")
	flag.Parse()
	task := strings.Join(flag.Args(), " ")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	c, err := h2a.Dial(ctx, h2a.Config{
		URL:    os.Getenv("H2A_URL"),
		Token:  os.Getenv("H2A_TOKEN"),
		Cursor: h2a.FileCursor(*seqFile),
		Logf:   log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	w := c.Welcome()

	if *to == "" && *conv == "" && !*listen {
		fmt.Printf("project %s, addresses:\n", w.Project)
		for _, a := range w.To {
			fmt.Printf("  %-12s %s\n", a.Address, a.Name)
		}
		return
	}
	if *to != "" && !known(w, *to) {
		log.Fatalf("%q is not an address of this pass: run without -to to list them", *to)
	}
	if task != "" && !*listen {
		acc, err := c.Send(ctx, h2a.Message{To: *to, Conversation: *conv, Text: task})
		if err != nil {
			log.Fatal(err)
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
	return false
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
