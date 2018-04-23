package streamcore_test

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamcore"
	"github.com/blendle/go-streamprocessor/streamutil/testutils"
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

			go streamcore.HandleErrors(ch, logger.Log)
			time.Sleep(testutils.MultipliedDuration(t, 20*time.Millisecond))

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
			ch := streamcore.ErrorsChan(make(chan error), tt.disabled)
			assert.Len(t, ch, tt.errors)
		})
	}
}
