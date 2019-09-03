package streamutil_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamutil"
	"github.com/blendle/go-streamprocessor/v3/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

type StubLogger struct {
	logs  []string
	mutex *sync.Mutex
}

func (l *StubLogger) Log(msg string, fields ...zapcore.Field) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	var log []string

	log = append(log, msg)
	for i := range fields {
		if err, ok := fields[i].Interface.(error); ok {
			log = append(log, err.Error())
		}
	}

	l.logs = append(l.logs, strings.Join(log, " "))
}

func (l *StubLogger) Logs() []string {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.logs
}

func TestHandleErrors(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		errs    []error
		results []string
	}{
		"single": {
			[]error{errors.New("panic!")},
			[]string{"Error received from streamclient. panic!"},
		},
		"multiple": {
			[]error{errors.New("panic!"), errors.New("seriously!")},
			[]string{
				"Error received from streamclient. panic!",
				"Error received from streamclient. seriously!",
			},
		},
		"skip nil": {
			[]error{nil, errors.New("seriously!")},
			[]string{"Error received from streamclient. seriously!"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			logger := &StubLogger{make([]string, 0), &sync.Mutex{}}
			ch := make(chan error, len(tt.errs))
			defer close(ch)

			for _, err := range tt.errs {
				ch <- err
			}

			go streamutil.HandleErrors(ch, logger.Log)
			time.Sleep(testutil.MultipliedDuration(t, 20*time.Millisecond))

			require.Len(t, logger.Logs(), len(tt.results))

			for i, s := range tt.results {
				assert.Equal(t, s, logger.Logs()[i])
			}
		})
	}
}

func TestErrorsChan(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		disabled bool
		errors   int
	}{
		"disabled": {
			true,
			1,
		},
		"enabled": {
			false,
			0,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ch := streamutil.ErrorsChan(make(chan error), tt.disabled)
			assert.Len(t, ch, tt.errors)
		})
	}
}

type errorCloserStub struct{ errors chan error }

func (ec *errorCloserStub) Errors() <-chan error { return ec.errors }
func (ec *errorCloserStub) Close() error         { close(ec.errors); return nil }

func TestErrors(t *testing.T) { // nolint: gocyclo
	t.Parallel()

	ec1 := &errorCloserStub{errors: make(chan error)}
	ec2 := &errorCloserStub{errors: make(chan error)}
	var ec3 stream.ErrorCloser

	ch := streamutil.Errors(ec1, ec2, ec3)

	go func() { ec1.errors <- errors.New("error 1") }()

	select {
	case err := <-ch:
		assert.Equal(t, err, errors.New("error 1"))
	case <-time.After(time.Second):
		t.Fatal("timeout while waiting for error to return")
	}

	go func() { ec1.errors <- errors.New("error 2") }()

	select {
	case err := <-ch:
		assert.Equal(t, err, errors.New("error 2"))
	case <-time.After(time.Second):
		t.Fatal("timeout while waiting for error to return")
	}

	go func() { ec2.errors <- errors.New("error 3") }()

	select {
	case err := <-ch:
		assert.Equal(t, err, errors.New("error 3"))
	case <-time.After(time.Second):
		t.Fatal("timeout while waiting for error to return")
	}

	// Send explicit nil error
	go func() { ec1.errors <- nil }()

	select {
	case err := <-ch:
		assert.Nil(t, err)
	case <-time.After(time.Second):
		t.Fatal("timeout while waiting for error to return")
	}

	// Don't send nil error if channel is closed
	go func() { require.NoError(t, ec1.Close()) }()

	select {
	case err := <-ch:
		t.Fatal("unexpected error returned: ", err)
	case <-time.After(10 * time.Millisecond):
	}
}
