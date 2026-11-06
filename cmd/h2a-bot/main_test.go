package main

import (
	"strings"
	"testing"

	h2a "github.com/harnesy/h2a-go"
)

// e2e on #4439: -item without -to on a pass of two projects sent a bare
// «lead», which the node refuses; the bot now says which lead to give.
func TestItemTargetNeedsToOnSeveralProjects(t *testing.T) {
	one := h2a.Welcome{Projects: []h2a.ProjectAccess{{Project: "App", To: []h2a.Address{{Address: "lead:1"}}}}}
	if to, err := itemTarget(one, ""); err != nil || to != "lead" {
		t.Fatalf("one project: %q %v", to, err)
	}
	if to, err := itemTarget(h2a.Welcome{}, ""); err != nil || to != "lead" {
		t.Fatalf("an old welcome: %q %v", to, err)
	}
	two := h2a.Welcome{Projects: []h2a.ProjectAccess{
		{Project: "App", To: []h2a.Address{{Address: "lead:1"}, {Address: "role:4"}}},
		{Project: "Site", To: []h2a.Address{{Address: "lead:2"}}},
	}}
	if _, err := itemTarget(two, ""); err == nil || !strings.Contains(err.Error(), "lead:1 (App)") || !strings.Contains(err.Error(), "lead:2 (Site)") {
		t.Fatalf("two projects: %v", err)
	}
	if to, err := itemTarget(two, "role:4"); err != nil || to != "role:4" {
		t.Fatalf("-to given: %q %v", to, err)
	}
}
