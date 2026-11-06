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

// The SDK reads every node frame of harnsy's session (the answers of
// control too: state, accepted of wake, sleep and item, h2a_not_yours), and
// writes its own hello, message, state, wake, sleep and item as the session
// has them.
func TestSessionIsHarnsys(t *testing.T) {
	var fs []struct {
		Dir   string          `json:"dir"`
		Frame json.RawMessage `json:"frame"`
	}
	if err := json.Unmarshal(vector(t, "session.json"), &fs); err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, r := range rules {
		known[r.Code] = true
	}
	sent := map[string]string{} // the service's frame type by id
	seen := map[string]int{}
	for i, x := range fs {
		var f frame
		if json.Unmarshal(x.Frame, &f) != nil {
			if x.Dir != "service" {
				t.Fatalf("frame %d of the node is not a frame: %s", i, x.Frame)
			}
			continue // the service's «not json»
		}
		k := x.Dir + ":" + f.Type
		var m struct {
			ID, To, Conversation, Project, Mode, Title, Text string
			Can                                              []string
			Parts                                            []map[string]any
		}
		json.Unmarshal(x.Frame, &m)
		if x.Dir == "service" && m.ID != "" {
			sent[m.ID] = f.Type
		}
		switch k {
		case "service:hello":
			var can []string
			for _, v := range m.Can {
				if v != "text" {
					can = append(can, v)
				}
			}
			ours, _ := json.Marshal(helloFrame{Type: "hello", V: Version, Token: "h2a_TOKEN", Can: withText(can)})
			sameJSON(t, i, ours, x.Frame)
		case "service:message":
			if text, ok := m.Parts[0]["text"].(string); ok {
				ours, _ := json.Marshal(messageFrame{Type: "message", ID: m.ID, To: m.To, Conversation: m.Conversation, Parts: []textPart{{text}}})
				sameJSON(t, i, ours, x.Frame)
			}
		case "service:state":
			ours, _ := json.Marshal(stateFrame{Type: "state", ID: m.ID, Project: m.Project})
			sameJSON(t, i, ours, x.Frame)
		case "service:wake", "service:sleep":
			ours, _ := json.Marshal(wakeFrame{Type: f.Type, ID: m.ID, To: m.To, Mode: m.Mode})
			sameJSON(t, i, ours, x.Frame)
		case "service:item":
			ours, _ := json.Marshal(itemFrame{Type: "item", ID: m.ID, To: m.To, Title: m.Title, Text: m.Text})
			sameJSON(t, i, ours, x.Frame)
		case "node:welcome":
			var w Welcome
			json.Unmarshal(x.Frame, &w)
			if w.V != Version || w.Project == "" || len(w.To) == 0 || w.Limits.TextBytes == 0 {
				t.Fatalf("frame %d: welcome %s", i, x.Frame)
			}
			if len(w.Projects) == 0 || w.Projects[0].Project == "" || len(w.Projects[0].Rights) == 0 || len(w.Projects[0].To) == 0 {
				t.Fatalf("frame %d: welcome.projects %+v", i, w.Projects)
			}
		case "node:accepted":
			ok := f.Re != ""
			switch sent[f.Re] {
			case "message":
				ok = ok && f.Conversation != ""
			case "item":
				ok = ok && f.Item > 0
			case "wake", "sleep":
			default:
				ok = false
			}
			if !ok {
				t.Fatalf("frame %d: accepted %s", i, x.Frame)
			}
			seen["node:accepted:"+sent[f.Re]]++
		case "node:state":
			s, err := stateOf(f)
			if err != nil || f.Re == "" || sent[f.Re] != "state" || len(s.Projects) == 0 || len(s.Projects[0].Posts) == 0 {
				t.Fatalf("frame %d: state %s (%v)", i, x.Frame, err)
			}
			for _, p := range s.Projects[0].Posts {
				if p.Address == "" || p.State == "" || p.Counts.Seats == 0 {
					t.Fatalf("frame %d: post %+v", i, p)
				}
			}
		case "node:message":
			if f.Seq == 0 || f.From == nil || f.From.Role == "" || f.text() == "" || f.Conversation == "" {
				t.Fatalf("frame %d: message %s", i, x.Frame)
			}
		case "node:error":
			if e := f.err(); !known[e.Code] {
				t.Fatalf("frame %d: error %s: a code the SDK does not know", i, x.Frame)
			}
			seen["node:error:"+f.Code]++
		case "node:pong", "service:ping", "service:bogus": // the session also sends a frame of no known kind
		default:
			t.Fatalf("frame %d: %s %s is not known to the SDK", i, x.Dir, f.Type)
		}
		seen[k]++
	}
	for _, k := range []string{"service:hello", "node:welcome", "service:message", "node:accepted", "node:message", "node:error",
		"service:state", "node:state", "service:wake", "service:sleep", "service:item",
		"node:accepted:wake", "node:accepted:sleep", "node:accepted:item", "node:error:" + CodeNotYours} {
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
