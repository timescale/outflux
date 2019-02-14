package utils

import (
	"fmt"
	"sync"
	"testing"
)

func TestBroadcastAlreadyClosed(t *testing.T) {
	// an open channel is in the registry
	state := idInRegistry("id")
	state.alreadyClosed = true
	bc := &defaultBroadcaster{state}
	// broadcast on already closed, no error is sent to channel
	bc.Broadcast("id2", fmt.Errorf("error"))
	close(state.registry["id"])
	broadcastErr := <-state.registry["id"]
	if broadcastErr != nil {
		t.Errorf("no error was expected, got: %v", broadcastErr)
	}
}

func TestBroadcastTwoChannels(t *testing.T) {
	// an open channel is in the registry
	state := twoIdsInRegistry("id", "id2")
	bc := &defaultBroadcaster{state}
	// broadcast only one channel receives the error, both get closed
	bc.Broadcast("id2", fmt.Errorf("error"))
	errFromChan1 := <-state.registry["id"]
	errFromChan2 := <-state.registry["id2"]
	if errFromChan1 == nil || errFromChan2 != nil {
		t.Error("expected only the first channel to receive an error")
	}

	if !state.alreadyClosed {
		t.Error("state should be closed after broadcast")
	}
}

func twoIdsInRegistry(id, id2 string) *state {
	reg := make(map[string]chan error)
	reg[id] = make(chan error, 1)
	reg[id2] = make(chan error, 1)
	return &state{reg, false, &sync.Mutex{}}
}
