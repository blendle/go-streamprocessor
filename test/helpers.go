package test

import "flag"

var Kafka *bool

func init() {
	Kafka = flag.Bool("kafka", false, "run tests with Kafka dependencies")
	flag.Parse()
}
