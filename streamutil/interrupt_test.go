package streamutil_test

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/v3/streamutil"
	"github.com/blendle/go-streamprocessor/v3/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestInterrupt(t *testing.T) {
	t.Parallel()

	if os.Getenv("RUN_WITH_EXEC") == "1" {
		println("go")

		select {
		case s := <-streamutil.Interrupt():
			println("interrupt received:", s.String())
			return
		case <-time.After(1 * time.Second):
		}

		return
	}

	var tests = []os.Signal{
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	}

	for _, tt := range tests {
		t.Run(tt.String(), func(t *testing.T) {
			want := "interrupt received: " + tt.String()

			out, err := testutil.Exec(t, "go", func(tb testing.TB, cmd *exec.Cmd) {
				require.NoError(t, cmd.Process.Signal(tt))
			})

			require.Nil(t, err)
			assert.Contains(t, out, want)
		})
	}
}

func TestInterrupt_NoSignal(t *testing.T) {
	t.Parallel()

	if os.Getenv("RUN_WITH_EXEC") == "1" {
		select {
		case <-streamutil.Interrupt():
		case <-time.After(100 * time.Millisecond):
			println("no signal")
		}

		return
	}

	out, err := testutil.Exec(t, "", nil)

	require.Nil(t, err)
	assert.Contains(t, out, "no signal")
}

func TestHandleInterrupts(t *testing.T) {
	t.Parallel()

	if os.Getenv("RUN_WITH_EXEC") == "1" {
		println("go")

		ch := make(chan os.Signal)
		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		fn := func() error {
			println("closed")
			return nil
		}

		go streamutil.HandleInterrupts(ch, fn, logger)

		time.Sleep(5 * time.Second)
		os.Exit(1)
	}

	var tests = []os.Signal{
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	}

	for _, tt := range tests {
		t.Run(tt.String(), func(t *testing.T) {
			out, err := testutil.Exec(t, "go", func(tb testing.TB, cmd *exec.Cmd) {
				require.NoError(t, cmd.Process.Signal(tt))
			})

			require.Nil(t, err)
			assert.Contains(t, out, fmt.Sprintf(`{"signal": "%s"}`, tt.String()))
		})
	}
}
