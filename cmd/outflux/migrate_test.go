package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/timescale/outflux/internal/pipeline"
)

func TestPreparePipeErrors(t *testing.T) {
	testCases := []struct {
		expected string
		input    []error
	}{
		{
			expected: "Migration finished with errors:\n",
			input:    []error{},
		}, {
			expected: "Migration finished with errors:\nOne\nTwo\n",
			input:    []error{fmt.Errorf("One"), fmt.Errorf("Two")},
		},
	}

	for _, testCase := range testCases {
		res := preparePipeErrors(testCase.input)
		if res.Error() != testCase.expected {
			t.Errorf("expected:%s\ngot:%s", testCase.expected, res.Error())
		}
	}
}
func TestMigrateNoPipes(t *testing.T) {
	app := &appContext{
		pipeService: &mockService{pipes: []pipeline.Pipe{}},
	}

	conn := &pipeline.ConnectionConfig{}
	mig := &pipeline.MigrationConfig{Quiet: true}
	err := migrate(app, conn, mig)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMigratePipeReturnsError(t *testing.T) {
	errorReturningPipe := &mockPipe{runErr: fmt.Errorf("error")}
	app := &appContext{
		pipeService: &mockService{
			pipes: []pipeline.Pipe{errorReturningPipe},
		},
	}
	conn := &pipeline.ConnectionConfig{}
	mig := &pipeline.MigrationConfig{MaxParallel: 1}
	err := migrate(app, conn, mig)
	if err == nil {
		t.Errorf("expected error, none received")
	} else if err[0].Error() != errorReturningPipe.runErr.Error() {
		t.Errorf("expected err %v, got %v", errorReturningPipe.runErr, err)
	}
}

func TestMigratePipesWaitForSemaphore(t *testing.T) {
	counter := &runCounter{lock: sync.Mutex{}}
	goodPipe1 := &mockPipe{counter: counter}
	goodPipe2 := &mockPipe{counter: counter}
	goodPipe3 := &mockPipe{counter: counter}
	app := &appContext{
		pipeService: &mockService{
			pipes: []pipeline.Pipe{goodPipe1, goodPipe2, goodPipe3},
		},
	}
	conn := &pipeline.ConnectionConfig{}
	mig := &pipeline.MigrationConfig{MaxParallel: 2}
	err := migrate(app, conn, mig)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if counter.maxRunning > int32(mig.MaxParallel) {
		t.Errorf("number of concurrent pipelines (%d) was too damn high (allowed %d)", counter.maxRunning, mig.MaxParallel)
	}
}
