package utils

import (
	"fmt"
	"testing"
)

func CheckError2(errorChannel chan error) error {
	select {
	case err := <-errorChannel:
		return err
	default:
		return nil
	}
}

func TestCheckError(t *testing.T) {
	testCases := []struct {
		desc     string
		expected error
		given    chan error
	}{
		{
			desc:     "Empty channel given, method doesn't block, returns nil",
			expected: nil,
			given:    make(chan error),
		}, {
			desc:     "Channel with error inside it given, returns that error",
			expected: fmt.Errorf("some error"),
			given:    mockChannelWithError(),
		},
	}

	got := CheckError(testCases[0].given)
	if got != nil {
		t.Errorf("Expected no error, got: %v", got)
	}

	got = CheckError(testCases[1].given)
	if got.Error() != testCases[1].expected.Error() {
		t.Errorf("Expected error: %v\nGot:%v", testCases[1].expected, got)
	}
}

func mockChannelWithError() chan error {
	cha := make(chan error, 1)
	cha <- fmt.Errorf("some error")
	return cha
}
