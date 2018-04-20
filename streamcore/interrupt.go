package streamcore

import (
	"os"
	"os/signal"
	"time"

	"go.uber.org/zap"
)

// HandleInterrupts monitors for an interrupt signal, and calls the provided
// closer function once received. It has a built-in timeout capability to force
// terminate the application when the closer takes too long to close, or returns
// an error during closing.
func HandleInterrupts(signals chan os.Signal, closer func() error, logger *zap.Logger) {
	signal.Notify(signals, os.Interrupt)

	s, ok := <-signals
	if !ok {
		return
	}

	logger.Info(
		"Got interrupt signal, cleaning up. Use ^C again to exit immediately.",
		zap.String("signal", s.String()),
	)

	go func() {
		abort := make(chan os.Signal, 1)
		signal.Notify(abort, os.Interrupt)
		<-abort

		os.Exit(1)
	}()

	go func() {
		time.Sleep(3 * time.Second)

		logger.Fatal(
			"Timed out while closing after receiving signal. Terminating.",
			zap.String("signal", s.String()),
		)
	}()

	err := closer()
	if err != nil {
		logger.Fatal(
			"Error while closing after receiving signal. Terminating.",
			zap.String("signal", s.String()),
			zap.Error(err),
		)
	}

	os.Exit(0)
}
