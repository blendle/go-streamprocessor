package standardstreamclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Client{}
}

func TestNew(t *testing.T) {
	t.Parallel()

	client, err := standardstreamclient.New()
	require.NoError(t, err)

	assert.Equal(t, "*standardstreamclient.Client", reflect.TypeOf(client).String())
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	options := func(c *streamconfig.Client) {
		c.Standardstream.Logger = logger
	}

	client, err := standardstreamclient.New(options)
	require.NoError(t, err)

	assert.EqualValues(t, logger, client.Config().Standardstream.Logger)
}
