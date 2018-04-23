package testutils_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamutil/testutils"
	"github.com/stretchr/testify/assert"
)

func TestIntegration_Test(t *testing.T) {
	testutils.Integration(t)
}

func TestVerbose(t *testing.T) {
	if ci, ok := os.LookupEnv("CI"); ok {
		_ = os.Unsetenv("CI")
		defer func() { _ = os.Setenv("CI", ci) }()
	}

	v := testutils.Verbose(t)

	assert.Equal(t, testing.Verbose(), v)
}

func TestVerbose_CI(t *testing.T) {
	_ = os.Setenv("CI", "true")
	defer func() { _ = os.Unsetenv("CI") }()

	assert.True(t, testutils.Verbose(t))
}

func TestRandom(t *testing.T) {
	s := testutils.Random(t)

	assert.True(t, strings.HasPrefix(s, "TestRandom-"), s)
}

func TestMultipliedDuration(t *testing.T) {
	var tests = map[string]struct {
		multiplier string
		in         time.Duration
		out        time.Duration
	}{
		"multiply by one": {
			"1",
			time.Duration(1 * time.Millisecond),
			time.Duration(1 * time.Millisecond),
		},

		"multiply by two": {
			"2",
			time.Duration(1 * time.Millisecond),
			time.Duration(2 * time.Millisecond),
		},

		"multiply by two (seconds)": {
			"2",
			time.Duration(1 * time.Second),
			time.Duration(2000 * time.Millisecond),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			curr := testutils.TestMultiplier
			defer func() { testutils.TestMultiplier = curr }()

			testutils.TestMultiplier = tt.multiplier
			actual := testutils.MultipliedDuration(t, tt.in)
			assert.Equal(t, tt.out, actual)
		})
	}
}

func TestMultipliedInt(t *testing.T) {
	var tests = map[string]struct {
		multiplier string
		in         int
		out        int
	}{
		"multiply by one": {
			"1",
			1,
			1,
		},

		"multiply by two": {
			"2",
			1,
			2,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			curr := testutils.TestMultiplier
			defer func() { testutils.TestMultiplier = curr }()

			testutils.TestMultiplier = tt.multiplier
			actual := testutils.MultipliedInt(t, tt.in)
			assert.Equal(t, tt.out, actual)
		})
	}
}
