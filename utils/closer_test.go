package utils

import "testing"

func TestClose(t *testing.T) {
	state := idInRegistry("id")
	state.alreadyClosed = true
	close(state.registry["id"])

	//will panic if Close attempts to close a closed channel
	cl := &defaultCloser{state}
	cl.Close()

	state = idInRegistry("id")
	cl = &defaultCloser{state}
	cl.Close()
	err := <-state.registry["id"]
	if err != nil {
		t.Errorf("channel should have been closed, and nil returned\ngot:%v", err)
	}
}
