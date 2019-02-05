package utils

import (
	"sync"
	"testing"
)

func TestSubscriber(t *testing.T) {
	testCases := []struct {
		state       *state
		id          string
		expectError bool
	}{
		{state: alreadyClosedState(), id: "id", expectError: true},
		{state: idInRegistry("id"), id: "id", expectError: true},
		{state: emptyState(), id: "id", expectError: false},
	}

	for _, tc := range testCases {
		sub := &defaultSubscriber{tc.state}
		res, err := sub.Subscribe(tc.id)
		if err != nil && !tc.expectError {
			t.Errorf("error wasn't expected. got: %v", err)
		}

		if err == nil && tc.expectError {
			t.Error("error was expected, none received")
		}

		if tc.expectError {
			continue
		}

		if tc.state.registry[tc.id] != res {
			t.Error("channel returned from Subscribe is not the same as in the registry")
		}
	}
}

func alreadyClosedState() *state {
	return &state{make(map[string]chan error), true, &sync.Mutex{}}
}

func idInRegistry(id string) *state {
	reg := make(map[string]chan error)
	reg[id] = make(chan error)
	return &state{reg, false, &sync.Mutex{}}
}

func emptyState() *state {
	return &state{make(map[string]chan error), false, &sync.Mutex{}}
}
