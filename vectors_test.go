package h2a

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"testing"
	"time"
)

// The contract's vectors, copied from harnsy unchanged (scripts/update-vectors.sh):
// the first line names the harnsy commit, the rest is harnsy's file.
func vector(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	head, body, ok := bytes.Cut(b, []byte("\n"))
	if !ok || !regexp.MustCompile(`^from harnsy [0-9a-f]{7,40}, contract "1"$`).Match(head) {
		t.Fatalf("%s: the first line %q does not name the harnsy commit", name, head)
	}
	return body
}

// The SDK's codes and moves are harnsy's codes.json, row by row, and so is
// the pause before a reconnect.
func TestCodesAreHarnsys(t *testing.T) {
	var v struct {
		Contract string `json:"contract"`
		Codes    []rule `json:"codes"`
		Backoff  struct {
			FirstSeconds int     `json:"first_seconds"`
			MaxSeconds   int     `json:"max_seconds"`
			Factor       float64 `json:"factor"`
			Jitter       float64 `json:"jitter"`
		} `json:"reconnect"`
	}
	if err := json.Unmarshal(vector(t, "codes.json"), &v); err != nil {
		t.Fatal(err)
	}
	if v.Contract != Version {
		t.Fatalf("contract %q", v.Contract)
	}
	if !reflect.DeepEqual(v.Codes, rules) {
		t.Fatalf("the rules differ from harnsy's codes.json:\nharnsy: %+v\nsdk:    %+v", v.Codes, rules)
	}
	b := newBackoff()
	if b.First != time.Duration(v.Backoff.FirstSeconds)*time.Second || b.Max != time.Duration(v.Backoff.MaxSeconds)*time.Second ||
		b.Factor != v.Backoff.Factor || b.Jitter != v.Backoff.Jitter {
		t.Fatalf("backoff %+v, harnsy %+v", b, v.Backoff)
	}
}

// The SDK reads every node frame of harnsy's session, and writes its own
// hello and message as the session has them.
func TestSessionIsHarnsys(t *testing.T) {
	var fs []struct {
		Dir   string          `json:"dir"`
		Frame json.RawMessage `json:"frame"`
	}
	if err := json.Unmarshal(vector(t, "session.json"), &fs); err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for i, x := range fs {
		var f frame
		if json.Unmarshal(x.Frame, &f) != nil {
			if x.Dir != "service" {
				t.Fatalf("frame %d of the node is not a frame: %s", i, x.Frame)
			}
			continue // the service's «not json»
		}
		seen[x.Dir+":"+f.Type]++
		switch x.Dir + ":" + f.Type {
		case "service:hello":
			ours, _ := json.Marshal(helloFrame{Type: "hello", V: Version, Token: "h2a_TOKEN", Can: []string{"text"}})
			sameJSON(t, i, ours, x.Frame)
		case "service:message":
			var m struct {
				ID, To, Conversation string
				Parts                []map[string]any
			}
			json.Unmarshal(x.Frame, &m)
			if text, ok := m.Parts[0]["text"].(string); ok {
				ours, _ := json.Marshal(messageFrame{Type: "message", ID: m.ID, To: m.To, Conversation: m.Conversation, Parts: []textPart{{text}}})
				sameJSON(t, i, ours, x.Frame)
			}
		case "node:welcome":
			var to []Address
			if f.V != Version || f.Project == "" || json.Unmarshal(f.To, &to) != nil || len(to) == 0 || f.Limits == nil || f.Limits.TextBytes == 0 {
				t.Fatalf("frame %d: welcome %s", i, x.Frame)
			}
		case "node:accepted":
			if f.Re == "" || f.Conversation == "" {
				t.Fatalf("frame %d: accepted %s", i, x.Frame)
			}
		case "node:message":
			if f.Seq == 0 || f.From == nil || f.From.Role == "" || f.text() == "" || f.Conversation == "" {
				t.Fatalf("frame %d: message %s", i, x.Frame)
			}
		case "node:error":
			if e := f.err(); e.Code == "" || frameMove(e.Code) == "" {
				t.Fatalf("frame %d: error %s", i, x.Frame)
			}
		case "node:pong", "service:ping", "service:bogus": // the session also sends a frame of no known kind
		default:
			t.Fatalf("frame %d: %s %s is not known to the SDK", i, x.Dir, f.Type)
		}
	}
	for _, k := range []string{"service:hello", "node:welcome", "service:message", "node:accepted", "node:message", "node:error"} {
		if seen[k] == 0 {
			t.Fatalf("the session has no %s: %v", k, seen)
		}
	}
}

func sameJSON(t *testing.T, i int, ours, theirs []byte) {
	t.Helper()
	var a, b any
	json.Unmarshal(ours, &a)
	json.Unmarshal(theirs, &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("frame %d: the SDK writes %s, the session has %s", i, ours, theirs)
	}
}
