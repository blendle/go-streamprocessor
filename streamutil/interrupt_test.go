package streamutil_test

import (
	"bufio"
	"bytes"
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
		case <-time.After(1 * time.Second):
			os.Exit(1)
		}

		return
	}

	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT} {
		cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
		cmd.Env = append(os.Environ(), "BE_TESTING_FATAL=1")

		var b bytes.Buffer
		cmd.Stderr = bufio.NewWriter(&b)
		require.NoError(t, cmd.Start())
		time.Sleep(100 * time.Millisecond)

		require.NoError(t, cmd.Process.Signal(sig))
		require.NoError(t, cmd.Wait())

		assert.Contains(t, b.String(), "interrupt received: "+sig.String())
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
		ch <- os.Interrupt

		time.Sleep(10 * time.Millisecond)

		return
	}

	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = append(os.Environ(), "BE_TESTING_FATAL=1")

	out, err := cmd.CombinedOutput()
	require.Nil(t, err, "output received: %s", string(out))

	assert.Contains(t, string(out), "Got interrupt signal")
	assert.Contains(t, string(out), "closed!")
}
