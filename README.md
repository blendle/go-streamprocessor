# go-streamprocessor [![Build Status](https://ci.blendle.io/buildStatus/icon?job=blendle/go-streamprocessor/master)](https://ci.blendle.io/job/blendle/job/go-streamprocessor/job/master/)

This package contains shared logic for Go-based stream processors.

## Usage

Import in your `main.go`:

```golang
import (
  "github.com/blendle/go-streamprocessor/stream"
  "github.com/blendle/go-streamprocessor/streamclient"
  "github.com/blendle/go-streamprocessor/streamclient/standardstream"
)
```

Next, instantiate a new consumer and producer (or one of both):

```golang
consumer, producer := streamclient.NewConsumerAndProducer(&standardstream.ClientConfig{})
```

Don't forget to close the producer once you're done:

```golang
defer producer.Close()
```

Next, loop over consumed messages, and produce new messages:

```golang
for msg := range consumer.Messages() {
  message := &stream.Message{}

  message.Value = append([]byte("processed: "), msg.Value...)

  producer.Messages() <- message
}
```

Now, run your example in your terminal:

```shell
echo -e "hello\nworld" | env DRY_RUN=true go run main.go
# processed: hello
# processed: world
```

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
cient := standardstream.NewClient(&standardstream.ClientConfig{})
consumer := client.NewConsumer()
producer := client.NewProducer()
```

### kafka

This stream client's consumer and producer listen and produce to Kafka streams.

```golang
cient := kafka.NewClient()
consumer := client.NewConsumer()
producer := client.NewProducer()
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
