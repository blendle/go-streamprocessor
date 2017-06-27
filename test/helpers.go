package test

import "flag"

// Kafka is set to `true` if `go test` is run with the `-kafka` flag.
//
// Use this to by-default skip tests that depend on a running Kafka instance:
//
//   if !*test.Kafka {
//     t.Skip()
//   }
var Kafka *bool

func init() {
	Kafka = flag.Bool("kafka", false, "run tests with Kafka dependencies")
	flag.Parse()
}
