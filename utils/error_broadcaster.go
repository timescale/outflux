package utils

import (
	"fmt"
	"sync"
)

type subscriber interface {
	Subscribe(id string) (chan error, error)
}

type unsubscriber interface {
	Unsubscribe(id string) error
}

type broadcaster interface {
	Broadcast(source string, err error)
}

type closer interface {
	Close()
}

// ErrorBroadcaster allows anything to subscribe to receive errors on a channel
type ErrorBroadcaster interface {
	subscriber
	unsubscriber
	broadcaster
	closer
}

// NewErrorBroadcaster creates a new instance of a specified type
func NewErrorBroadcaster() ErrorBroadcaster {
	registry := make(map[string]chan error)
	state := &state{
		registry:      registry,
		alreadyClosed: false,
		lock:          &sync.Mutex{},
	}
	return &defaultErrorBroadcaster{
		subscriber:   &defaultSubscriber{state},
		unsubscriber: &defaultUnsubscriber{state},
		broadcaster:  &defaultBroadcaster{state},
		closer:       &defaultCloser{state},
	}
}

type defaultErrorBroadcaster struct {
	subscriber
	unsubscriber
	broadcaster
	closer
}
type state struct {
	registry      map[string]chan error
	alreadyClosed bool
	lock          *sync.Mutex
}
type defaultSubscriber struct {
	state *state
}

func (sub *defaultSubscriber) Subscribe(id string) (chan error, error) {
	state := sub.state
	state.lock.Lock()
	defer state.lock.Unlock()
	if state.alreadyClosed {
		return nil, fmt.Errorf("error subscriber is already closed")
	}

	if _, exists := state.registry[id]; exists {
		return nil, fmt.Errorf("id %s already registered for error updates", id)
	}

	newChannel := make(chan error, 1)
	state.registry[id] = newChannel
	return newChannel, nil
}

type defaultUnsubscriber struct {
	state *state
}

func (sub *defaultUnsubscriber) Unsubscribe(id string) error {
	var channel chan error
	var exists bool

	state := sub.state
	state.lock.Lock()
	defer state.lock.Unlock()

	if channel, exists = state.registry[id]; !exists {
		return fmt.Errorf("id %s not registered for error updates", id)
	}

	delete(state.registry, id)

	if !state.alreadyClosed {
		close(channel)
	}

	return nil
}

type defaultBroadcaster struct {
	state *state
}

func (sub defaultBroadcaster) Broadcast(source string, err error) {
	state := sub.state
	state.lock.Lock()
	defer state.lock.Unlock()

	if state.alreadyClosed {
		return
	}

	for subID, errChannel := range state.registry {
		if subID != source {
			errChannel <- err
		}

		close(errChannel)
	}

	state.alreadyClosed = true
}

type defaultCloser struct {
	state *state
}

func (sub defaultCloser) Close() {
	state := sub.state
	state.lock.Lock()
	defer state.lock.Unlock()

	if state.alreadyClosed {
		return
	}

	for _, errChannel := range state.registry {
		close(errChannel)
	}
}
