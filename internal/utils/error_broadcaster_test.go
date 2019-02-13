package utils

import "testing"

func TestNewErrorBroadcaster(t *testing.T) {
	errorBroadcaster := NewErrorBroadcaster()

	castBroadcaster := errorBroadcaster.(*defaultErrorBroadcaster)

	subscriber := castBroadcaster.subscriber.(*defaultSubscriber)
	unsubscriber := castBroadcaster.unsubscriber.(*defaultUnsubscriber)
	broadcaster := castBroadcaster.broadcaster.(*defaultBroadcaster)
	closer := castBroadcaster.closer.(*defaultCloser)
	subState := subscriber.state

	if subState != unsubscriber.state || subState != broadcaster.state || subState != closer.state {
		t.Errorf("state is not the same in all constituents")
	}

	if len(subState.registry) != 0 {
		t.Errorf("registry is not empty")
	}

	if subState.alreadyClosed {
		t.Errorf("new error broadcaster should not be closed")
	}
}
