package utils

import (
	"testing"
)

func TestUnsubscriber(t *testing.T) {
	a := make(map[string]bool)
	delete(a, "kure")
	testCases := []struct {
		state       *state
		id          string
		expectError bool
	}{
		{state: emptyState(), id: "id", expectError: true},
		{state: idInRegistry("id"), id: "id", expectError: false},
	}

	for _, tc := range testCases {
		sub := &defaultUnsubscriber{tc.state}
		err := sub.Unsubscribe(tc.id)
		if err != nil && !tc.expectError {
			t.Errorf("error wasn't expected. got: %v", err)
		}

		if err == nil && tc.expectError {
			t.Error("error was expected, none received")
		}

		if tc.expectError {
			continue
		}

		if len(tc.state.registry) != 0 {
			t.Error("registry was not empty after unsubscribe")
		}
	}
}

func TestUnsubscribeWhenAlreadyClosed(t *testing.T) {
	state := idInRegistry("id")
	state.alreadyClosed = true
	close(state.registry["id"])

	// if already closed, don't close the channel again
	sub := &defaultUnsubscriber{state}
	err := sub.Unsubscribe("id")
	if err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if len(state.registry) != 0 {
		t.Errorf("expected registry to be empty, got: %v", state.registry)
	}
}
