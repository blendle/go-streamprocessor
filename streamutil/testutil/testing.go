package testutil

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var validIntegrationTestName = regexp.MustCompile(`^(Test|Benchmark)(Integration)?.+$`)
var validRandomName = regexp.MustCompile(`[a-zA-Z0-9_]+`)

// TestMultiplier can be used to increase the default duration or integers used
// during test runs when waiting for time-sensitive values to be returned. It
// defaults to a multiplier of 1.
//
// This is specifically useful on slower environments like a CI server.
var TestMultiplier = "1"

// Integration skips the test if the `-short` flag is specified. It also
// enforces the test name to starts with `Integration`, this allows to run
// _only_ integration tests using `-run '^TestIntegration'` or
// `-bench '^BenchmarkIntegration`.
func Integration(tb testing.TB) {
	tb.Helper()

	match := validIntegrationTestName.FindStringSubmatch(tb.Name())
	if len(match) < 3 || match[2] == "" {
		tb.Fatalf("integration test name %s does not start with %sIntegration", tb.Name(), match[1])
	}

	if testing.Short() {
		tb.Skip("integration test skipped due to -short")
	}
}

// Verbose returns true if the `testing.Verbose` flag returns true, or if the
// environment variable `CI` is set to any value. This allows you to see more
// verbose error reporting locally when running `go test -v ./...`, or on the CI
// by default.
func Verbose(tb testing.TB) bool {
	tb.Helper()

	if testing.Verbose() {
		return true
	}

	return os.Getenv("CI") != ""
}

// MultipliedDuration returns the provided duration, multiplied by the
// configured `TestMultiplier` variable.
func MultipliedDuration(tb testing.TB, d time.Duration) time.Duration {
	tb.Helper()

	m, err := strconv.Atoi(TestMultiplier)
	if err != nil {
		tb.Fatalf("unable to convert TestMultiplier to integer: %s", TestMultiplier)
	}

	return time.Duration(d.Nanoseconds() * int64(m))
}

// MultipliedInt returns the provided int, multiplied by the configured
// `TestMultiplier` variable.
func MultipliedInt(tb testing.TB, i int) int {
	tb.Helper()

	m, err := strconv.Atoi(TestMultiplier)
	if err != nil {
		tb.Fatalf("unable to convert TestMultiplier to integer: %s", TestMultiplier)
	}

	return i * m
}

// Random returns a random string, to use during testing.
func Random(tb testing.TB) string {
	tb.Helper()

	rand.Seed(time.Now().Unix())
	name := strings.Replace(tb.Name(), "/", "_", -1)
	name = validRandomName.FindString(name)
	return fmt.Sprintf("%s-%d", name, rand.Intn(math.MaxInt64))
}
