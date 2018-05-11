package testutil_test

import (
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Test(t *testing.T) {
	testutil.Integration(t)
}

func TestExec(t *testing.T) {
	t.Parallel()

	if os.Getenv("RUN_WITH_EXEC") == "1" {
		println("go")
		println("done")
		return
	}

	out, err := testutil.Exec(t, "go", func(tb testing.TB, cmd *exec.Cmd) {})

	require.Nil(t, err)
	assert.Contains(t, out, "go\ndone")
}

func TestLogger(t *testing.T) {
	assert.Equal(t, "*zap.Logger", reflect.TypeOf(testutil.Logger(t)).String())
}

func TestVerbose_TEST_DEBUG(t *testing.T) {
	if ci, ok := os.LookupEnv("CI"); ok {
		_ = os.Unsetenv("CI")
		defer func() { _ = os.Setenv("CI", ci) }()
	}

	_ = os.Setenv("TEST_DEBUG", "true")
	defer func() { _ = os.Unsetenv("CI") }()

	assert.True(t, testutil.Verbose(t))
}

func TestVerbose_CI(t *testing.T) {
	if test, ok := os.LookupEnv("TEST_DEBUG"); ok {
		_ = os.Unsetenv("TEST_DEBUG")
		defer func() { _ = os.Setenv("TEST_DEBUG", test) }()
	}

	_ = os.Setenv("CI", "true")
	defer func() { _ = os.Unsetenv("CI") }()

	assert.True(t, testutil.Verbose(t))
}

func TestRandom(t *testing.T) {
	s := testutil.Random(t)

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
			curr := testutil.TestMultiplier
			defer func() { testutil.TestMultiplier = curr }()

			testutil.TestMultiplier = tt.multiplier
			actual := testutil.MultipliedDuration(t, tt.in)
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
			curr := testutil.TestMultiplier
			defer func() { testutil.TestMultiplier = curr }()

			testutil.TestMultiplier = tt.multiplier
			actual := testutil.MultipliedInt(t, tt.in)
			assert.Equal(t, tt.out, actual)
		})
	}
}

func TestWithMultiplier(t *testing.T) {
	var tests = map[string]struct {
		multiplier float64
		fn         func(tb testing.TB)
	}{
		"set TestMultiplier to 1": {
			1,
			func(tb testing.TB) { require.Equal(tb, "1.0000", testutil.TestMultiplier) },
		},

		"set TestMultiplier to 2": {
			2,
			func(tb testing.TB) { require.Equal(tb, "2.0000", testutil.TestMultiplier) },
		},

		"set TestMultiplier to 0.5": {
			0.5,
			func(tb testing.TB) { require.Equal(tb, "0.5000", testutil.TestMultiplier) },
		},

		"without function": {
			0.5,
			nil,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			testutil.WithMultiplier(t, tt.multiplier, tt.fn)
		})
	}
}
