package h2a

import (
	"context"
	"os"
	"testing"
	"time"
)

// Against a real node (a trial node with the gateway and harnsy Max, a pass
// to the lead): H2A_TEST_URL, H2A_TEST_TOKEN; without them the test is
// skipped. With H2A_TEST_TASK it gives that task to the first address and
// waits for one answer (H2A_TEST_WAIT, default 5m).
func TestLiveNode(t *testing.T) {
	url, token := os.Getenv("H2A_TEST_URL"), os.Getenv("H2A_TEST_TOKEN")
	if url == "" || token == "" {
		t.Skip("H2A_TEST_URL and H2A_TEST_TOKEN are not set")
	}
	wait := 5 * time.Minute
	if d, err := time.ParseDuration(os.Getenv("H2A_TEST_WAIT")); err == nil {
		wait = d
	}
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	c, err := Dial(ctx, Config{URL: url, Token: token, Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w := c.Welcome()
	if w.V != Version || len(w.To) == 0 {
		t.Fatalf("welcome %+v", w)
	}
	task := os.Getenv("H2A_TEST_TASK")
	if task == "" {
		return
	}
	acc, err := c.Send(ctx, Message{To: w.To[0].Address, Text: task})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("accepted, conversation %s", acc.Conversation)
	err = c.Run(ctx, func(ctx context.Context, m Reply) error {
		t.Logf("[%s · %s] %s", m.From.Role, m.From.Project, m.Text)
		cancel()
		return nil
	})
	if err != context.Canceled {
		t.Fatal(err)
	}
}
