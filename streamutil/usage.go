package streamutil

import (
	"bytes"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"text/template"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/kelseyhightower/envconfig"
)

// About provides details about the processor and its configuration.
type About struct {
	Name,
	URI,
	Usage string

	Consumers,
	Producers []string

	Config map[string]string
}

type name struct {
	Upper, Lower, Usage, Line string
}

type usageData struct {
	Name,
	NameLine,
	Usage,
	URI string

	Consumers,
	Producers []name

	Config map[string][]string

	Count int
}

// TODO:
// - split up into multiple templates
// - colorize headers
// - colorize environment variables
// - properly split lines at 80 characters
// - indent anything other than headers by 2 spaces
// - underline documentation link
// - add configurable "EXAMPLE" header at the top
// - improve consumer/producer environment variable output
//   - show default values
//   - show descriptions
//   - show required true/false
//   - use same format as "PROCESSOR CONFIGURATION"
const usage = `
{{ .Name }}
{{ .NameLine }}

{{ .Usage }}

{{ if .URI }}
🐚 ONLINE DOCUMENTATION
-----------------------

For more details about this processor, see the following URL:

    {{ .URI }}
{{ end }}
{{ if .Count }}
🐝 USAGE
--------

This stream processor is configured via environment variables. The list of
available variables is split between these {{ .Count }} sections:

{{ if .Config -}}
* PROCESSOR CONFIGURATION – how to configure the business logic of this
  processor via environment variables.
{{ end }}

{{- range .Consumers }}
* {{ .Upper }} – how to configure the {{if eq .Lower "consumer"}}default {{end}}{{.Lower}}
{{ end }}

{{- range .Producers }}
* {{ .Upper }} – how to configure the {{if eq .Lower "producer"}}default {{end}}{{.Lower}}
{{ end }}

{{ if .Config -}}
⚙️  PROCESSOR CONFIGURATION
--------------------------
{{ range $k, $v := .Config }}
{{ $k }}
{{- range $v }}
    {{ . }}
{{- end }}
{{ end }}
{{ end }}
{{- range .Consumers }}
⚙️  {{ .Upper }}
---{{ .Line }}

In order to use this consumer, you need to set the environment variable
"STREAMCLIENT_CONSUMER" to one of the following values:

* standardstream
* inmem
* kafka
* pubsub

Alternatively, if you pipe any input to this consumer over stdin, and don't set
the "STREAMCLIENT_CONSUMER" environment variable, the consumer mode will be set
to "standardstream".

In any other situation, the consumer will terminate.

{{ .Usage }}
{{ end }}
{{- range .Producers }}
⚙️  {{ .Upper }}
---{{ .Line }}

In order to use this producer, you need to set the environment variable
"STREAMCLIENT_PRODUCER" to one of the following values:

* standardstream
* inmem
* kafka
* pubsub

Alternatively, if you set the environment variable "DRY_RUN" to any value, and
don't set the "STREAMCLIENT_PRODUCER" environment variable, the producer mode
will be set to "standardstream".

In any other situation, the producer will terminate.

{{ .Usage }}
{{ end }}
{{ end }}
`

// Usage listens for `--help` and prints the relevant details on how to use the
// processor. A boolean is returned indicating if the usage documentation is
// printed, at which point the processor should probably exit.
func Usage(out io.Writer, about About) bool {
	if len(os.Args) < 2 {
		return false
	}

	if os.Args[1] != "--help" && os.Args[1] != "-h" {
		return false
	}

	count := len(about.Consumers) + len(about.Producers)
	if len(about.Config) > 0 {
		count++
	}

	var consumers []name
	for _, c := range about.Consumers {
		b := &bytes.Buffer{}
		tabs := tabwriter.NewWriter(b, 1, 0, 4, ' ', 0)
		conf := &streamconfig.Consumer{}

		_ = envconfig.Usagef(c, conf, tabs, envconfig.DefaultTableFormat) // nolint
		_ = tabs.Flush()                                                  // nolint

		cc := strings.Replace(c, "_", " ", -1)
		consumers = append(consumers, name{
			Lower: cc,
			Upper: strings.ToUpper(cc),
			Usage: b.String(),
			Line:  strings.Repeat("-", len(cc)),
		})
	}

	var producers []name
	for _, p := range about.Producers {
		b := &bytes.Buffer{}
		tabs := tabwriter.NewWriter(b, 1, 0, 4, ' ', 0)
		conf := &streamconfig.Producer{}

		_ = envconfig.Usagef(p, conf, tabs, envconfig.DefaultTableFormat) // nolint
		_ = tabs.Flush()                                                  // nolint

		pp := strings.Replace(p, "_", " ", -1)
		producers = append(producers, name{
			Lower: pp,
			Upper: strings.ToUpper(pp),
			Usage: b.String(),
			Line:  strings.Repeat("-", len(pp)),
		})
	}

	config := map[string][]string{}
	for k, v := range about.Config {
		config[k] = strings.Split(v, "\n")
	}

	data := &usageData{
		Name:      about.Name,
		NameLine:  strings.Repeat("=", len(about.Name)),
		Usage:     about.Usage,
		URI:       about.URI,
		Consumers: consumers,
		Producers: producers,
		Config:    config,
		Count:     count,
	}

	t := template.Must(template.New("usage").Parse(usage))
	buf := &bytes.Buffer{}
	if err := t.Execute(buf, data); err != nil {
		panic(err)
	}

	_, err := out.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}

	return true
}
