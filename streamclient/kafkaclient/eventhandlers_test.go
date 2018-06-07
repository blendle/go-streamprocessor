package kafkaclient

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func TestHandleError(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		err     kafka.ErrorCode
		ignores []kafka.ErrorCode
		out     string
	}{
		"no ignore":     {kafka.ErrBadMsg, []kafka.ErrorCode{}, "Local: Bad message format"},
		"ignored":       {kafka.ErrBadMsg, []kafka.ErrorCode{kafka.ErrBadMsg}, ""},
		"multi-ignored": {kafka.ErrBadMsg, []kafka.ErrorCode{kafka.ErrFail, kafka.ErrBadMsg}, ""},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {

			err := testErr{errors.New(tt.err.String()), tt.err}
			ch := make(chan error, 100)
			logger := zap.NewNop()

			handleError(err, tt.ignores, ch, logger)

			if tt.out == "" {
				assert.Len(t, ch, 0)
				return
			}

			got := <-ch

			assert.Equal(t, got.Error(), "received error from event stream: "+tt.out)
		})
	}
}

type testErr struct {
	err  error
	code kafka.ErrorCode
}

func (e testErr) Error() string         { return e.err.Error() }
func (e testErr) Code() kafka.ErrorCode { return e.code }
