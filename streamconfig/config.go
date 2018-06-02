package streamconfig

import (
	"bytes"
	"text/tabwriter"

	"github.com/kelseyhightower/envconfig"
)

// TODO: implement `--help` by default, and add `EnableHelpFlag`.

func usage(name string, inf interface{}) []byte {
	var err error
	b := &bytes.Buffer{}
	tabs := tabwriter.NewWriter(b, 1, 0, 4, ' ', 0)

	if c, ok := inf.(Consumer); ok {
		err = envconfig.Usagef(name, &c, tabs, envconfig.DefaultTableFormat)
	}

	if p, ok := inf.(Producer); ok {
		err = envconfig.Usagef(name, &p, tabs, envconfig.DefaultTableFormat)
	}

	if err != nil {
		return nil
	}

	if tabs.Flush() != nil {
		return nil
	}

	return b.Bytes()
}
