package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIngestorConfigParse(t *testing.T) {
	x, err := ParseStrategyString(CommitOnEnd.String())
	assert.Equal(t, CommitOnEnd, x)
	assert.NoError(t, err)
	x, err = ParseStrategyString(CommitOnEachBatch.String())
	assert.Equal(t, CommitOnEachBatch, x)
	assert.NoError(t, err)
	x, err = ParseStrategyString("anything else")
	assert.Error(t, err)
}
