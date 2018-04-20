package streamcore_test

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamcore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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

		go streamcore.HandleInterrupts(ch, fn, logger)
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
