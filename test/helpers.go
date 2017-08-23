package test

import "os"

// Kafka is set to `true` if the `KAFKA` environment variable is set to `true`.
//
// Use this to by-default skip tests that depend on a running Kafka instance:
//
//   if !test.Kafka {
//     t.Skip()
//   }
var Kafka bool

func init() {
	if os.Getenv("KAFKA") == "true" {
		Kafka = true
	}
}
