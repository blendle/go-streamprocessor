package testutils_test

import (
	"strings"
	"testing"

	"github.com/blendle/go-streamprocessor/streamutils/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTimeoutMultiplier(t *testing.T) {
	assert.Equal(t, 1, testutils.TimeoutMultiplier)
}

func TestIntegration_Test(t *testing.T) {
	testutils.Integration(t)
}

func TestRandom(t *testing.T) {
	s := testutils.Random(t)

	assert.True(t, strings.HasPrefix(s, "TestRandom-"), s)
}
