# go-streamprocessor [![Build Status](https://travis-ci.org/blendle/go-streamprocessor.svg?branch=master)](https://travis-ci.org/blendle/go-streamprocessor)

This package contains shared logic for Go-based stream processors.

* [Description](#description)
  * [Messages](#messages)
  * [Stream Clients](#stream-clients)
    * [Kafka](#kafka)
    * [Inmem](#inmem)
    * [Standardstream](#standardstream)
  * [Shared Interface](#shared-interface)
* [Usage](#usage)
  * [Initialization](#initialization)
  * [Consuming](#consuming)
  * [Producing](#producing)
  * [Error Handling](#error-handling)
  * [Multiple Clients](#multiple-clients)
* [Stream Options](#stream-options)

## Description

The `go-streamprocessor` package includes utilities to work with streams of data
from different sources. You can consume messages, have your business logic use
or manipulate the incoming data, then optionally produce a message to the next
stream.

The library revolves around two main core objects:

* a `stream.Message` struct to handle existing, or create new messages
* a `stream.Consumer` and `stream.Producer` interface to abstract away how to
  consumer or produce those messages, allowing for multiple client
  implementations.

### Messages

Let's first take a look at the `Message` struct:

```golang
type Message struct {
  Value         []byte
  Key           []byte
  Timestamp     time.Time
  Tags          map[string][]byte
  ConsumerTopic string
  ProducerTopic string
  Offset        *int64
}
```

You can use any of the above fields to influence the outcome of your
application.

It is important to note that not all stream client implementations support all
of the `Message` fields. For example, the `standardstream` client implementation
only supports the `Value` field, whereas the `kafka` implementation supports all
of the fields. You need to take this into account while developing your
application, and either show an error when you depend on a field that isn't set,
or use alternative solutions to deal with such a situation.

### Stream Clients

This library supports the following stream clients. More will follow in the
future.

#### Kafka

When using the Kafka client provided by this package, you can consume messages
from one or more topics, act on the message properties such as its key, value,
timestamp, or tags, and produce the same (or a new) message to a different topic
using the Kafka producer.

#### Inmem

The inmem client provides easy access to the interface of the consumer and
producer, allowing you to test your business logic, without depending on
external systems like Kafka.

Combined with the inmem client, there's a `inmemstore` package that allows you
to prepare a "stream" of data to consume from, or prepare a store to which your
application will produce its messages, you can then inspect the contents of the
store to confirm that the outcome is as expected.

#### Standardstream

Another stream client implementation used as an aid during development. This
client can consume from, and write to, a file descriptor. This allows you to
test your processor locally by piping in data, and outputting the results to
stdout.

### Shared Interface

You can support all client implementations in your application because of the
interface they all share. This allows you to use the `kafka` client in
production, use the `inmem` client in your tests, and the `standardstream`
client as live examples in your documentation, while still using the same
application business logic in all three situations.

## Usage

Let's first look at a full implementation to consume from any stream.

If you have a file `main.go`, with the following contents:

```golang
package main

import (
    "fmt"

    "github.com/blendle/go-streamprocessor/streamclient"
)

func main() {
    consumer, err := streamclient.NewConsumer()
    if err != nil {
        panic(err)
    }

    for msg := range consumer.Messages() {
        fmt.Printf("%s\n", msg.Value)
    }
}
```

If you run `echo -e 'hello\nworld' | go run main.go`, you will get the following
output:

```shell
hello
world
```

What just happened?

### Initialization

First, we initialized a new consumer, like so:

```golang
streamclient.NewConsumer()
```

Using the generic `streamclient` package means that we don't restrict this
application to any specific _type_ of stream clients, we instead leave this up
to the user to decide which client type to use.

In this case, because we piped data to the application using `echo | myapp`,
`NewConsumer()` determined that the `standardstream` client implementation is
the best client to deal with this type of stream and instantiated that specific
client.

If we were to run our application without piping data, we would have received an
error, because there is no contextual information for the initializer to know
which type of stream client implementation you want to use:

```shell
$ go run main.go
panic: unable to determine required consumer streamclient
```

In this case, we have to tell the consumer what client to use, by setting the
correct environment variable:

```shell
env CONSUMER_CLIENT_TYPE=standardstream go run main.go
```

Now, we don't get an error, but instead, the application started, but doesn't
stop anymore. Why is this?

The reason for this is that piping data into the application means there is a
clear beginning and end of the stream, and the `standardstream` client is
configured so that it stops the consumer once it reaches that end of the stream.

It's also possible to have a continuous stream of data coming into the client,
which is what we did in this case. Since we didn't send any data _yet_, the
consumer considers the stream to be continuous, and thus keeps a connection to
be able to read future data.

In fact, if we now type any characters, ending with a newline, the consumer will
consume that message straight away, and our application prints the exact same
string as its output:

```shell
hello
hello
```

After you're done, you can close the consumer using `^C`.

> **pro tip:** instead of using
> `env CONSUMER_CLIENT_TYPE=standardstream go run main.go`, you could have also
> used `cat | go run main.go`. Since `cat` opens a stream without closing it,
> the end result will be the same, except that the consumer detected a unix
> pipe, so it knows that you want to use the `standardstream` client
> implementation.

### Consuming

Let's take a closer look at the part where we consume the incoming data:

```golang
for msg := range consumer.Messages() { /* ... */ }
```

`Messages()` is a method signature part of our `stream.Consumer` interface:

```golang
Messages() <-chan Message
```

As you can see, it returns a channel that delivers `stream.Message` objects for
you to use. This method is implemented by all stream clients, so you know that
your application can always consume messages in a uniform way, regardless of the
used client.

### Producing

Our basic example showed how to consume messages. What if we wanted to also
produce messages? Let's take a look at an extended version of the previous
example, with a diff of the changes we'll make:

```diff
diff --git a/main.go b/main.go
index aed2e2f..d82ce72 100644
--- a/main.go
+++ b/main.go
@@ -1,8 +1,6 @@
 package main

 import (
-    "fmt"
-
     "github.com/blendle/go-streamprocessor/streamclient"
 )

@@ -13,7 +11,13 @@ func main() {
     }
     defer consumer.Close()

+    producer, err := streamclient.NewProducer()
+    if err != nil {
+        panic(err)
+    }
+    defer producer.Close()
+
     for msg := range consumer.Messages() {
-        fmt.Printf("%s\n", msg.Value)
+        producer.Messages() <- msg
    }
 }
```

As you can see, we've removed the `fmt` printer, and replaced it with a new
stream producer.

This producer also has a `Messages()` method signature, which is part of the
`stream.Producer` interface:

```golang
Messages() chan<- Message
```

The difference with the consumer implementation is that the producer accepts new
messages on a channel, and produces them to a stream, whereas the consumer
sends you messages it received from a stream.

If you run the modified code the same way as before, you'll now get an error:

```shell
$ echo -e 'hello\nworld' | go run main.go
panic: unable to determine required producer streamclient
```

In this case, there is no way for the producer to know which client
implementation you want to use, so you have to tell it explicitly:

```shell
$ echo -e 'hello\nworld' | env PRODUCER_CLIENT_TYPE=standardstream go run main.go
hello
world
```

Nothing has changed in the output, but under the hood, the `standardstream`
client implementation received the messages you sent it via its `Messages()`
channel, and has delivered them to the `stdout` stream.

> **pro tip:** instead of using
> `env PRODUCER_CLIENT_TYPE=standardstream go run main.go`, you could have also
> used `env DRY_RUN=1 go run main.go`. This is a shortcut which basically means
> "don't do anything dangerous with the data you are going to produce, instead
> always use the `standardstream` producer and produce the outcome to `stdout`".

### Error Handling

The above examples have been relatively simple. However, in the last example we
introduced a producer that functions independent of the already initialized
consumer. When you have an application that has multiple consumers and/or
producers, you need to make sure you handle any errors returned by these stream
clients correctly, allowing the clients to close cleanly before you terminate
the application.

By default, consumers and producers keep all of their error handling internal.
If an error occurs, they will log the error, close the client, and cause the
application to terminate with exit status 1.

Because each client operates individually, an error in one client, cannot be
acted upon by a different client, so even though the client that received the
error will shut down gracefully and only then terminate the application, any
other client won't have the opportunity to close gracefully, and might terminate
while in the midst of delivering or receiving messages, potentially causing a
loss of data.

To deal with this, you should handle errors manually in your application. Let's
take another diff from the last example to see how to do that:

```diff
diff --git a/main.go b/main.go
index d82ce72..a5811b3 100644
--- a/main.go
+++ b/main.go
@@ -1,23 +1,39 @@
 package main

 import (
+    "fmt"
+
     "github.com/blendle/go-streamprocessor/streamclient"
+    "github.com/blendle/go-streamprocessor/streamconfig"
+    "github.com/blendle/go-streamprocessor/streamutil"
 )

 func main() {
-    consumer, err := streamclient.NewConsumer()
+    consumer, err := streamclient.NewConsumer(streamconfig.ManualErrorHandling())
     if err != nil {
         panic(err)
     }
     defer consumer.Close()

-    producer, err := streamclient.NewProducer()
+    producer, err := streamclient.NewProducer(streamconfig.ManualErrorHandling())
     if err != nil {
         panic(err)
     }
     defer producer.Close()

-    for msg := range consumer.Messages() {
-        producer.Messages() <- msg
+    errs := streamutil.Errors(consumer, producer)
+
+    for {
+        select {
+        case msg, ok := <-consumer.Messages():
+            if !ok {
+                return
+            }
+
+            producer.Messages() <- msg
+        case err := <-errs:
+            fmt.Printf("received error: %s", err)
+            return
+        }
     }
 }
```

We've made several changes to support manual error handling:

* We initialize the consumer and producer, using the
  `streamconfig.ManualErrorHandling()` option. This allows us to manually
  intercept stream errors, and disables any automated error handling by the
  clients.

* We use a small helper utility `streamutil.Errors` to capture all errors
  produced by any of the clients, and send them to us over a channel.

* And we changed the for loop, into a for-select loop that waits for either a
  message from the stream, or an error from either the producer or consumer, and
  act accordingly.

By catching the errors ourselves, we can call `return` and have our deferred
client close calls be triggered as expected, making sure the clients close
correctly.

We can run this example the same as before, and the outcome still remains the
same, as long as no error occurs:

```golang
$ echo -e 'hello\nworld' | env DRY_RUN=1 go run main.go
hello
world
```

If we do manage to receive an error, we handle that as well:

```golang
$ echo '<long string causing buffer overflow>' | env DRY_RUN=1 go run main.go
received error: unable to read message from stream: bufio.Scanner: token too long
```

In this case, we traded correctness for some complexity. If you need to use
a single consumer or producer, there's no need to handle errors manually, but
anything more than one can result in data loss without manual error handling.

### Multiple Clients

Previous examples either used a single consumer, or combine a single consumer
with a single producer. In those situations, it's easy to configure these
clients through environment variables, by either prefixing the variables (as
described below) with `CONSUMER_` or `PRODUCER_`.

If we wanted to use two producers in one application, we would extended the
above example as follows:

```diff
diff --git a/main.go b/main.go
index 7f24877..d438267 100644
--- a/main.go
+++ b/main.go
@@ -27,7 +27,16 @@ func main() {
     }
     defer producer.Close()

-    errs := streamutil.Errors(consumer, producer)
+    producer2, err := streamclient.NewProducer(
+            streamconfig.ManualErrorHandling(),
+            streamconfig.Name("producer2"),
+    )
+    if err != nil {
+            panic(err)
+    }
+    defer producer2.Close()
+
+    errs := streamutil.Errors(consumer, producer, producer2)

     for {
         select {
@@ -36,6 +45,11 @@ func main() {
                 return
             }

+            if msg.Timestamp.UnixNano()%2 == 0 {
+                    producer2.Messages() <- msg
+                    break
+            }
+
             producer.Messages() <- msg
         case err := <-errs:
             fmt.Printf("received error: %s", err)
```

In the above example, we used `streamconfig.Name("producer2")` to set the name
of the second producer to `producer2`. We can then use the `PRODUCER2_` prefix
to configure this individual producer, and continue to use `PRODUCER_` to
configure the original producer.

The same logic applies to consumers.

## Stream Options

In the usage example, the `streamconfig.ManualErrorHandling()` option was
demonstrated. This is one of many options you can pass into a new consumer or
producer, to configure it the way you want.

Aside from configuring clients via code, you can also delegate the configuration
to the user of you application, who can use environment variables to configure
each client individually.

| function signature                                                           | C/P | environment variable                 | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------- | --- | ------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ConsumerOptions(fn func(c *Consumer))`                                      | C   | -                                    | A convenience accessor to manually set consumer options.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `ProducerOptions(fn func(p *Producer))`                                      | P   | -                                    | A convenience accessor to manually set producer options.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `DisableEnvironmentConfig()`                                                 | *   | -                                    | Prevents the consumer or producer to be configured via environment variables, instead of the default configuration to allow environment variable-based configurations.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `ManualErrorHandling()`                                                      | *   | -                                    | Prevents the consumer or producer to automatically handle stream errors. When this option is passed, the application itself needs to listen to, and act on the `Errors()` channel.                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `ManualInterruptHandling()`                                                  | *   | -                                    | Prevents the consumer or producer to automatically handle interrupt signals. When this option is passed, the application itself needs to handle Unix interrupt signals to properly close the consumer or producer when required.                                                                                                                                                                                                                                                                                                                                                                                             |
| `Logger(l *zap.Logger)`                                                      | *   | -                                    | Sets the logger for the consumer or producer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `Name(s string)`                                                             | *   | -                                    | Sets the name for the consumer or producer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `InmemListen()`                                                              | C   | `INMEM_CONSUME_ONCE=false`           | Configures the inmem consumer to continuously listen for any new messages in the configured store.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `InmemStore(s stream.Store)`                                                 | *   | -                                    | Adds a store to the inmem consumer and producer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `KafkaBroker(s string)`                                                      | *   | `KAFKA_BROKERS=broker1,broker2`      | Adds a broker to the list of configured Kafka brokers.<br><br>Brokers is a list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping — this list only impacts the initial hosts used to discover the full set of servers. Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), you do not need to add the full set of servers (you may want more than one, though, in case a server is down). |
| `KafkaCommitInterval(d time.Duration)`                                       | C   | `KAFKA_COMMIT_INTERVAL=30s`          | Sets the consumer's CommitInterval.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `KafkaCompressionCodec(s kafkaconfig.Compression)`                           | P   | `KAFKA_COMPRESSION_CODEC=snappy`     | Sets the compression codec for the produced messages.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `KafkaDebug()`                                                               | *   | `KAFKA_DEBUG=all`                    | Enabled debugging for Kafka.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `KafkaGroupID(s string)`                                                     | C   | `KAFKA_GROUP_ID=hello`               | Sets the group ID for the consumer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `KafkaGroupIDRandom()`                                                       | C   | -                                    | Sets the group ID for the consumer to a random ID. This can be used to configure one-off consumers that should not share their state in a consumer group.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `KafkaHeartbeatInterval(d time.Duration)`                                    | *   | `KAFKA_HEARTBEAT_INTERVAL=10s`       | Sets the consumer or producer HeartbeatInterval.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `KafkaID(s string)`                                                          | *   | `KAFKA_CLIENT_ID=hello`              | Sets the consumer or producer ID.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `KafkaMaxDeliveryRetries(i int)`                                             | P   | `KAFKA_MAX_DELIVERY_RETRIES=1`       | Sets the MaxDeliveryRetries.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `KafkaMaxInFlightRequests(i int)`                                            | *   | `KAFKA_MAX_IN_FLIGHT_REQUESTS=1`     | Sets the maximum allowed in-flight requests for both consumers and producers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `KafkaMaxQueueBufferDuration(d time.Duration)`                               | P   | `KAFKA_MAX_QUEUE_BUFFER_DURATION=2m` | Sets the MaxQueueBufferDuration                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `KafkaMaxQueueSizeKBytes(i int)`                                             | P   | `KAFKA_MAX_QUEUE_SIZE_KBYTES=512`    | Sets the MaxQueueSizeKBytes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `KafkaMaxQueueSizeMessages(i int)`                                           | P   | `KAFKA_MAX_QUEUE_SIZE_MESSAGES=10`   | Sets the MaxQueueSizeMessages.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `KafkaOffsetHead(i uint32)`                                                  | C   | `KAFKA_OFFSET_DEFAULT=10`            | sets the OffsetDefault to a positive number.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `KafkaOffsetInitial(s kafkaconfig.Offset)`                                   | C   | `KAFKA_OFFSET_INITIAL=beginning`     | Sets the OffsetInitial.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `KafkaOffsetTail(i uint32)`                                                  | C   | `KAFKA_OFFSET_DEFAULT=-10`           | Sets the OffsetDefault to a negative number (starting at the last offset).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `KafkaOrderedDelivery()`                                                     | P   | -                                    | Sets `MaxInFlightRequests` to `1` for the producer, to guarantee ordered delivery of messages.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `KafkaRequireNoAck()`                                                        | P   | `KAFKA_REQUIRED_ACKS=0`              | Configures the producer not to wait for any broker acks.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `KafkaRequireLeaderAck()`                                                    | P   | `KAFKA_REQUIRED_ACKS=1`              | Configures the producer wait for a single ack by the Kafka cluster leader broker.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `KafkaRequireAllAck()`                                                       | P   | `KAFKA_REQUIRED_ACKS=-1`             | Configures the producer wait for a acks from all brokers available in the Kafka cluster.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `KafkaSecurityProtocol(s kafkaconfig.Protocol)`                              | *   | `KAFKA_SECURITY_PROTOCOL=ssl`        | Configures the producer or consumer to use the specified security protocol.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `KafkaSessionTimeout(d time.Duration)`                                       | *   | `KAFKA_SESSION_TIMEOUT=5m`           | Configures the producer or consumer to use the specified session timeout.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `KafkaSSL(capath, cert, crl, kpassword, key, kstorepassword, kstore string)` | *   | `KAFKA_SSL_*`                        | Configures the producer or consumer to use the specified SSL config.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `KafkaStatisticsInterval(d time.Duration)`                                   | *   | `KAFKA_STATISTICS_INTERVAL=1h`       | Configures the producer or consumer to use the specified statistics interval.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `KafkaTopic(s string)`                                                       | *   | `KAFKA_TOPIC=hello`                  | Configures the producer or consumer to use the specified topic. In case of the consumer, this option can be used multiple times to consume from more than one topic. In case of the producer, the last usage of this option will set the final topic to produce to.                                                                                                                                                                                                                                                                                                                                                          |
