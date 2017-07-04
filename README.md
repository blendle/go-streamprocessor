# go-streamprocessor [![Build Status](https://ci.blendle.io/buildStatus/icon?job=blendle/go-streamprocessor/master)](https://ci.blendle.io/blue/organizations/jenkins/blendle%2Fgo-streamprocessor/activity?branch=master)

This package contains shared logic for Go-based stream processors.

## Usage

Import in your `main.go`:

```golang
import (
  "github.com/blendle/go-streamprocessor/stream"
  "github.com/blendle/go-streamprocessor/streamclient"
)
```

Next, instantiate a new consumer and producer (or one of both):

```golang
consumer, producer := streamclient.NewConsumerAndProducer()
```

Don't forget to close the producer once you're done:

```golang
defer producer.Close()
```

Next, loop over consumed messages, and produce new messages:

```golang
for msg := range consumer.Messages() {

  // Initialize a new message.
  message := &stream.Message{}

  // Set the value of the message to the consumed message, prepended with
  // "processed: ".
  message.Value = append([]byte("processed: "), msg.Value...)

  // Send message to the configured producer.
  producer.Messages() <- message

  // Mark the consumed message as "done". This lets the client know it should
  // not retry this message if the application is restarted.
  msg.Done()
}
```

Don't forget to mark the consumed message as "done", or you might end up
processing the message multiple times.

Now, run your example in your terminal:

```shell
echo -e "hello\nworld" | env DRY_RUN=true go run main.go
# processed: hello
# processed: world
```

You can find the above example in the [examples directory](examples/).

## Client implementations

In the above example, we used the generic `streamclient.NewConsumerAndProducer`.
This creates a consumer and producer, based on the environment you run the
program in.

In our example, we send data to the program over `stdin`, this triggered the
application to initialize the `standardstream` consumer, which listens on the
`stdin` stream for any messages (split by newlines).

We also set the `DRY_RUN` environment variable to `true`, this causes the
`standardstream` _producer_ to be started as well, which produces messages on
the `stdout` stream.

There are multiple stream processor clients available. You can initialize the
one you want by creating a consumer or producer from the correct client.

### standardstream

This stream client's consumer listens to stdin, while the producer sends
messages to stdout.

```golang
client := standardstream.NewClient()
consumer := client.NewConsumer()
producer := client.NewProducer()
```

You can tell the consumer or producer to listen or write to another file
descriptor as well:

```golang
options := func(c *standardstream.Client) {
  c.ConsumerFD, _ = os.Open("/var/my/file")
  c.ProducerFD = os.Stderr
}

client := standardstream.NewClient(options)
```

### kafka

This stream client's consumer and producer listen and produce to Kafka streams.

```golang
client := kafka.NewClient()
consumer := client.NewConsumer()
producer := client.NewProducer()
```

If you set the `KAFKA_CONSUMER_URL` and/or `KAFKA_PRODUCER_URL` environment
variables, the client will configure the broker details using those values.

For example:

    KAFKA_CONSUMER_URL="kafka://localhost:9092/my-topic?group=my-group"
    KAFKA_PRODUCER_URL="kafka://localhost:9092/my-other-topic"

You can also define these values using client options:

```golang
options := func(c *kafka.Client) {
  c.ConsumerBrokers = []string{"localhost"}
  c.ConsumerTopics = []string{"my-topic"}
  c.ConsumerGroup = "my-group"

  c.ProducerBrokers = []string{"localhost"}
  c.ProducerTopics  = []string{"my-other-topic"}
}

client := kafka.NewClient(options)
```

### Inmem

This client receives the messages from an in-memory store, and produces it to
the same (or another) in-memory store.

This client is mostly useful during testing, so you reduce the dependency on
file descriptors, or Kafka itself.

```golang
client := inmem.NewClient()
consumer := client.NewConsumer()
producer := client.NewProducer()
```

Here are some more use-cases:

```golang
// create a new inmemory store, so we can access it and pass it to our new inmem
// client.
store := inmem.NewStore()

// create a new topic in the store
topic := store.NewTopic("input-topic")

// add a single message to the topic.
topic.NewMessage([]byte("hello world!"), []byte("my-partition-key"))

// set custom configuration.
options := func(c *Client) {
  c.ConsumerTopic = "input-topic"
  c.ProducerTopic = "output-topic"
}

client := inmem.NewClientWithStore(store, options)
consumer := client.NewConsumer()

for msg := range consumer.Messages() {
  fmt.Printf("received: %s", string(msg))
}

// received: hello world
```

## Logging

You can pass a pointer to a `zap.Logger` instance to each of the above described
clients.

```golang
logger := func(c *kafka.Client) {
  c.Logger, _ = zap.NewProduction()
}

client := kafka.NewClient(logger)
```

## Messages

A message is implemented as such:

```golang
type Message struct {
  Value     []byte
  Timestamp time.Time
}
```

You can set the value of the message, and a timestamp (depending on the client,
this could be used, or not).

You can also set a "partition key" for a message. Again, depending on the type
of streamclient you are using, this is either used, or it isn't.

Here's an example:

```golang
producer.PartitionKey(func(msg *stream.Message) []byte {
  return getKey(msg.Value)
})
```

In the above example, we implement our own `getKey` function that takes `[]byte`
as its input, and returns the key string as `[]byte`.

## TODO

- [ ] add unit tests for Kafka streamclient (e.g. missing offset commit).
