package streamutil

import (
	"errors"

	"github.com/blendle/go-streamprocessor/stream"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Errors takes a list of stream consumers and/or producers, and returns a
// combined errors channel on which any errors reported by the processors are
// delivered.
func Errors(errs ...stream.ErrorCloser) <-chan error {
	errChan := make(chan error)

	for _, e := range errs {
		go func(c <-chan error) {
			for {
				errChan <- (<-c)
			}
		}(e.Errors())
	}

	return errChan
}

// HandleErrors listens to the provided channel, and triggers a fatal error when
// any error is received.
func HandleErrors(ch chan error, logger func(msg string, fields ...zapcore.Field)) {
	for err := range ch {
		if err == nil {
			continue
		}

		logger("Error received from streamclient.", zap.Error(err))
	}
}

// ErrorsChan returns the passed in errors channel, unless `disabled` is set to
// `true`, in which case a temporary new errors channel is created, and an error
// is returned, indicating that the errors channel cannot be used with the
// current streamconfig.
func ErrorsChan(ch chan error, disabled bool) chan error {
	if disabled {
		errs := make(chan error, 1)
		defer close(errs)

		errs <- errors.New("unable to manually consume errors while HandleErrors is true")
		return errs
	}

	return ch
}
