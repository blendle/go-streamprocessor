package streamclient

// Type represents the streamclient implementation type.
type Type string

const (
	// Unknown represents an unknown streamclient implementation.
	Unknown Type = "unknown"

	// Inmem represents the in-memory streamclient implementation.
	Inmem = "inmem"

	// Kafka represents the Kafka streamclient implementation.
	Kafka = "kafka"

	// Standardstream represents the Standardstream streamclient implementation.
	Standardstream = "standardstream"
)

func (t Type) String() string {
	switch t {
	case Inmem:
		fallthrough
	case Kafka:
		fallthrough
	case Standardstream:
		return string(t)
	}

	return string(Unknown)
}
