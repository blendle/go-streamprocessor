package inmemclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = inmemclient.Client{}
}

func TestNew(t *testing.T) {
	t.Parallel()

	client, err := inmemclient.New()
	require.NoError(t, err)

	assert.Equal(t, "*inmemclient.Client", reflect.TypeOf(client).String())
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	options := func(c *streamconfig.Client) {
		c.Inmem.Logger = logger
	}

	client, err := inmemclient.New(options)
	require.NoError(t, err)

	assert.EqualValues(t, logger, client.Config().Inmem.Logger)
}
