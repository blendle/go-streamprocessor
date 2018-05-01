package streamutil_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestInterrupt(t *testing.T) {
	t.Parallel()

	if os.Getenv("BE_TESTING_FATAL") == "1" {
		select {
		case s := <-streamutil.Interrupt():
			println("interrupt received:", s.String())
			return
		case <-time.After(5 * time.Second):
			os.Exit(1)
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
			cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
			cmd.Env = append(os.Environ(), "BE_TESTING_FATAL=1")

			var b bytes.Buffer
			cmd.Stderr = bufio.NewWriter(&b)
			require.NoError(t, cmd.Start())
			time.Sleep(150 * time.Millisecond)

			require.NoError(t, cmd.Process.Signal(tt))
			require.NoError(t, cmd.Wait())

			assert.Contains(t, b.String(), "interrupt received: "+tt.String())
		})
	}
}

func TestHandleInterrupts(t *testing.T) {
	t.Parallel()

	if os.Getenv("BE_TESTING_FATAL") == "1" {
		ch := make(chan os.Signal)
		logger, err := zap.NewDevelopment()
		require.NoError(t, err)

		fn := func() error {
			println("closed!")
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
			cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
			cmd.Env = append(os.Environ(), "BE_TESTING_FATAL=1")

			var b bytes.Buffer
			cmd.Stderr = bufio.NewWriter(&b)
			require.NoError(t, cmd.Start())
			time.Sleep(150 * time.Millisecond)

			require.NoError(t, cmd.Process.Signal(tt))
			require.NoError(t, cmd.Wait())

			assert.Contains(t, b.String(), fmt.Sprintf(`{"signal": "%s"}`, tt.String()))
		})
	}
}
