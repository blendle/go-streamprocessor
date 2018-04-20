package testutils_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamutils/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTimeoutMultiplier(t *testing.T) {
	assert.Equal(t, 1, testutils.TimeoutMultiplier)
}
