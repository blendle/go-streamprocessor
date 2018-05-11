package testutil

import (
	"bufio"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var validIntegrationTestName = regexp.MustCompile(`^(Test|Benchmark)(Integration)?.+$`)
var validRandomName = regexp.MustCompile(`[a-zA-Z0-9_]+`)

// TestMultiplier can be used to increase the default duration or integers used
// during test runs when waiting for time-sensitive values to be returned. It
// defaults to a multiplier of 1.0.
//
// This is specifically useful on slower environments like a CI server.
var TestMultiplier = "1.0"

// Exec runs the current executed test as an external command, setting the
// `RUN_WITH_EXEC` environment variable to `1`, waiting for the `wait` string to
// be printed on stderr (unless empty), executes the `runner` function, and
// returns the stderr output and the exit status of the command.
func Exec(tb testing.TB, wait string, runner func(tb testing.TB, cmd *exec.Cmd)) (string, error) {
	tb.Helper()

	var out1, out2 []string
	var wg sync.WaitGroup

	cmd := exec.Command(os.Args[0], "-test.run="+tb.Name()) // nolint:gas
	cmd.Env = append(os.Environ(), "RUN_WITH_EXEC=1")

	stderr, err := cmd.StderrPipe()
	require.NoError(tb, err)

	stdout, err := cmd.StdoutPipe()
	require.NoError(tb, err)

	require.NoError(tb, cmd.Start())

	wg.Add(2)
	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			out1 = append(out1, scanner.Text())
		}
	}()

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			s := scanner.Text()

			if runner != nil && wait == s {
				time.Sleep(10 * time.Millisecond)
				runner(tb, cmd)
			}

			out2 = append(out2, s)
		}
	}()

	wg.Wait()

	return strings.Join(append(out1, out2...), "\n"), cmd.Wait()
}

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

// Logger returns a Zap logger instance to use during testing. It returns logs
// in a user-friendly format, reporting anything above warn level.
func Logger(tb testing.TB) *zap.Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level.SetLevel(zap.ErrorLevel)

	log, err := cfg.Build()
	require.NoError(tb, err)

	return log
}

// Verbose returns true if the `testing.Verbose` flag returns true, or if the
// environment variable `CI` is set to any value. This allows you to see more
// verbose error reporting locally when running `go test -v ./...`, or on the CI
// by default.
func Verbose(tb testing.TB) bool {
	tb.Helper()

	if _, ok := os.LookupEnv("TEST_DEBUG"); ok {
		return true
	}

	return os.Getenv("CI") != ""
}

// MultipliedDuration returns the provided duration, multiplied by the
// configured `TestMultiplier` variable.
func MultipliedDuration(tb testing.TB, d time.Duration) time.Duration {
	tb.Helper()

	m, err := strconv.ParseFloat(TestMultiplier, 64)
	if err != nil {
		tb.Fatalf("unable to convert TestMultiplier to float: %s", TestMultiplier)
	}

	return time.Duration(float64(d.Nanoseconds()) * m)
}

// MultipliedInt returns the provided int, multiplied by the configured
// `TestMultiplier` variable. `TestMultiplier` is a float, but is converted to
// the next logical integer (ceiling).
func MultipliedInt(tb testing.TB, i int) int {
	tb.Helper()

	m, err := strconv.ParseFloat(TestMultiplier, 64)
	if err != nil {
		tb.Fatalf("unable to convert TestMultiplier to float: %s", TestMultiplier)
	}

	return i * int(math.Ceil(m))
}

// Random returns a random string, to use during testing.
func Random(tb testing.TB) string {
	tb.Helper()

	rand.Seed(time.Now().Unix())
	name := strings.Replace(tb.Name(), "/", "_", -1)
	name = validRandomName.FindString(name)
	return fmt.Sprintf("%s-%d", name, rand.Intn(math.MaxInt64))
}

// WithMultiplier runs the passed in function with the configured test
// multiplier. Note that this function is NOT thread-safe. If you call this
// function in tests that set `Parallel()`, you might get unexpected results.
func WithMultiplier(tb testing.TB, f float64, fn func(tb testing.TB)) {
	if fn == nil {
		return
	}

	tm := TestMultiplier
	defer func() { TestMultiplier = tm }()

	TestMultiplier = fmt.Sprintf("%.4f", f)
	fn(tb)
}
