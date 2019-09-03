package kafkaconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/v3/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebug_Set(t *testing.T) {
	var tests = map[string]struct {
		in  string
		out kafkaconfig.Debug
	}{
		"empty": {
			"",
			kafkaconfig.Debug{},
		},

		"all": {
			"all",
			kafkaconfig.Debug{All: true},
		},

		"broker": {
			"broker",
			kafkaconfig.Debug{Broker: true},
		},

		"cgrp": {
			"cgrp",
			kafkaconfig.Debug{CGRP: true},
		},

		"consumer": {
			"consumer",
			kafkaconfig.Debug{Consumer: true},
		},

		"feature": {
			"feature",
			kafkaconfig.Debug{Feature: true},
		},

		"fetch": {
			"fetch",
			kafkaconfig.Debug{Fetch: true},
		},

		"generic": {
			"generic",
			kafkaconfig.Debug{Generic: true},
		},

		"interceptor": {
			"interceptor",
			kafkaconfig.Debug{Interceptor: true},
		},

		"metadata": {
			"metadata",
			kafkaconfig.Debug{Metadata: true},
		},

		"msg": {
			"msg",
			kafkaconfig.Debug{Msg: true},
		},

		"plugin": {
			"plugin",
			kafkaconfig.Debug{Plugin: true},
		},

		"protocol": {
			"protocol",
			kafkaconfig.Debug{Protocol: true},
		},

		"queue": {
			"queue",
			kafkaconfig.Debug{Queue: true},
		},

		"security": {
			"security",
			kafkaconfig.Debug{Security: true},
		},

		"topic": {
			"topic",
			kafkaconfig.Debug{Topic: true},
		},

		"combination": {
			"queue,topic,msg",
			kafkaconfig.Debug{Queue: true, Topic: true, Msg: true},
		},

		"combination with all": {
			"queue,all,topic,msg",
			kafkaconfig.Debug{All: true},
		},

		"unknown": {
			"unknown",
			kafkaconfig.Debug{},
		},

		"combination with unknown": {
			"metadata,unknown,feature",
			kafkaconfig.Debug{Metadata: true, Feature: true},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			d := &kafkaconfig.Debug{}

			err := d.Set(tt.in)
			require.NoError(t, err)

			assert.EqualValues(t, tt.out, *d)
		})
	}
}

func TestOffset_Set(t *testing.T) {
	var tests = map[string]struct {
		in  string
		out kafkaconfig.Offset
	}{
		"empty": {
			"",
			"",
		},

		"beginning": {
			"beginning",
			kafkaconfig.OffsetBeginning,
		},

		"end": {
			"end",
			kafkaconfig.OffsetEnd,
		},

		"unknown": {
			"unknown",
			"unknown",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			oo := kafkaconfig.Offset("")
			o := &oo

			err := o.Set(tt.in)
			require.NoError(t, err)

			assert.EqualValues(t, tt.out, *o)
		})
	}
}

func TestProtocol_Set(t *testing.T) {
	var tests = map[string]struct {
		in  string
		out kafkaconfig.Protocol
	}{
		"empty": {
			"",
			"",
		},

		"plaintext": {
			"plaintext",
			kafkaconfig.ProtocolPlaintext,
		},

		"ssl": {
			"ssl",
			kafkaconfig.ProtocolSSL,
		},

		"sasl_plaintext": {
			"sasl_plaintext",
			kafkaconfig.ProtocolSASLPlaintext,
		},

		"sasl_ssl": {
			"sasl_ssl",
			kafkaconfig.ProtocolSASLSSL,
		},

		"unknown": {
			"unknown",
			"unknown",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			oo := kafkaconfig.Protocol("")
			o := &oo

			err := o.Set(tt.in)
			require.NoError(t, err)

			assert.EqualValues(t, tt.out, *o)
		})
	}
}

func TestCompression_Set(t *testing.T) {
	var tests = map[string]struct {
		in  string
		out kafkaconfig.Compression
	}{
		"empty": {
			"",
			"",
		},

		"none": {
			"none",
			kafkaconfig.CompressionNone,
		},

		"gzip": {
			"gzip",
			kafkaconfig.CompressionGZIP,
		},

		"snappy": {
			"snappy",
			kafkaconfig.CompressionSnappy,
		},

		"lz4": {
			"lz4",
			kafkaconfig.CompressionLZ4,
		},

		"unknown": {
			"unknown",
			"unknown",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			oo := kafkaconfig.Compression("")
			o := &oo

			err := o.Set(tt.in)
			require.NoError(t, err)

			assert.EqualValues(t, tt.out, *o)
		})
	}
}
