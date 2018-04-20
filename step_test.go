package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/DATA-DOG/godog"
	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	_ "github.com/blendle/go-streamprocessor/test"
)

type Run struct {
	Consumer      stream.Consumer
	CloseProducer chan bool
	msgCount      int
	msgTemplate   string
}

func FeatureContext(s *godog.Suite) {
	r := &Run{}

	s.BeforeScenario(func(interface{}) {
		r.Consumer = nil
		r.msgCount = 0
		r.msgTemplate = `hello from message %d!`
	})

	s.Step(`^the topic "([^"]*)" exists$`, r.theTopicExists)
	s.Step(`^the "([^"]*)" message exists in topic "([^"]*)"$`, r.theMessageExistsInTopic)
	s.Step(`^the kafka consumer consumes from the "([^"]*)" topic$`, r.theKafkaConsumerConsumesFromTheTopic)
	s.Step(`^(\d+) messages? should have been consumed$`, r.messagesShouldHaveBeenConsumed)
	s.Step(`^(\d+) messages? exists? in topic "([^"]*)"$`, r.messagesExistInTopic)
	s.Step(`^messages are continuously streamed into the "([^"]*)" topic$`, r.messagesAreContinuouslyStreamedIntoTheTopic)
	s.Step(`^the kafka consumer closes after (\d+) seconds?$`, r.theKafkaConsumerClosesAfterSeconds)
	s.Step(`^no more messages are streamed into the topic$`, r.noMoreMessagesAreStreamedIntoTheTopic)
	s.Step(`^all messages should have been consumed$`, r.allMessagesShouldHaveBeenConsumed)
	s.Step(`^the messages are consumed in the correct order$`, r.theMessagesAreCorrectlyOrdered)
}

func (r *Run) theTopicExists(name string) error {
	return createKafkaTopic(name)
}

func (r *Run) theMessageExistsInTopic(message, topic string) error {
	r.addMessagesToTopic(1, message, topic)

	return nil
}

func (r *Run) theKafkaConsumerConsumesFromTheTopic(topic string) error {
	options := func(c *kafka.Client) {
		c.ConsumerBrokers = []string{"localhost:9092"}
		c.ConsumerTopics = []string{topic}
		c.ConsumerGroup = "my-group"

		if debug() {
			c.Logger, _ = zap.NewDevelopment()
		}
	}

	if debug() {
		sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	client := kafka.NewClient(options)
	r.Consumer = client.NewConsumer()

	return nil
}

func (r *Run) messagesShouldHaveBeenConsumed(count int) error {
	defer r.Consumer.Close()

	timeout := &time.Timer{}
	i := 0
Loop:
	for {
		select {
		case <-r.Consumer.Messages():
			if i == 0 {
				timeout = time.NewTimer(500 * time.Millisecond)
			} else {
				timeout.Reset(500 * time.Millisecond)
			}

			i++
		case <-timeout.C:
			break Loop
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timeout waiting for messages to be consumed")
		}
	}

	if i != count {
		return fmt.Errorf("unexpected number of messages comsumed: %d (expected %d)", i, count)
	}

	return nil
}

func (r *Run) theMessagesAreCorrectlyOrdered() error {
	defer r.Consumer.Close()

	timeout := &time.Timer{}
	i := 0
Loop:
	for {
		select {
		case msg := <-r.Consumer.Messages():
			if i == 0 {
				timeout = time.NewTimer(500 * time.Millisecond)
			} else {
				timeout.Reset(500 * time.Millisecond)
			}

			expected := fmt.Sprintf(r.msgTemplate, i+1)
			if string(msg.Value) != expected {
				return fmt.Errorf("unexpected message value: %s (expected %s)", string(msg.Value), expected)
			}

			i++
		case <-timeout.C:
			break Loop
		case <-time.After(30 * time.Second):
			return fmt.Errorf("timeout waiting for messages to be consumed")
		}
	}

	return nil
}

func (r *Run) messagesExistInTopic(count int, topic string) error {
	r.addMessagesToTopic(count, r.msgTemplate, topic)

	return nil
}

func (r *Run) messagesAreContinuouslyStreamedIntoTheTopic(topic string) error {
	go func() {
		r.CloseProducer = make(chan bool)

	ProducerLoop:
		for i := 1; i > 0; i++ {
			select {
			case <-r.CloseProducer:
				break ProducerLoop
			default:
				r.addMessagesToTopic(1, fmt.Sprintf(r.msgTemplate, i), topic)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return nil
}

func (r *Run) theKafkaConsumerClosesAfterSeconds(seconds int) error {
	time.Sleep(time.Duration(seconds) * time.Second)

	return r.Consumer.Close()
}

func (r *Run) noMoreMessagesAreStreamedIntoTheTopic() error {
	r.CloseProducer <- true
	close(r.CloseProducer)

	return nil
}

func (r *Run) allMessagesShouldHaveBeenConsumed() error {
	return r.messagesShouldHaveBeenConsumed(r.msgCount)
}

/******************\
    TEST HELPERS
\******************/

func (r *Run) addMessagesToTopic(count int, message, topic string) {
	if message == "" {
		message = r.msgTemplate
	}

	broker := newBroker()

	runs := 1
	if count > 10000 {
		runs = count / 1000
		count = count / runs
	}

	for j := 0; j < runs; j++ {
		request := sarama.ProduceRequest{}
		request.RequiredAcks = sarama.WaitForLocal

		for i := 1; i <= count; i++ {
			msg := message
			if strings.Count(message, "%d") == 1 {
				msg = fmt.Sprintf(message, i+(10*j))
			}

			request.AddMessage(topic, 0, &sarama.Message{Codec: sarama.CompressionNone, Key: nil, Value: []byte(msg)})
			r.msgCount++
		}

		response, err := broker.Produce(&request)
		if err != nil {
			println(err.Error())
		}
		if response == nil {
			println("Produce request without NoResponse got no response!")
		}
	}
}

func newBroker() *sarama.Broker {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		panic(err)
	}

	return broker
}

func kafkaTopicExists(name string) bool {
	broker := newBroker()
	defer broker.Close()

	res, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		panic(err)
	}

	exists := false
	for _, t := range res.Topics {
		if t.Name == name {
			exists = true
			break
		}
	}

	return exists
}

func createKafkaTopic(name string) error {
	var err error

	if kafkaTopicExists(name) {
		if err = kafkaDeleteTopicCmd(name); err != nil {
			return err
		}
	}

	err = kafkaCreateTopicCmd(name)

	if err != nil && strings.Contains(err.Error(), fmt.Sprintf("Topic '%s' already exists", name)) {
		err = createKafkaTopic(name)
	}

	return err
}

func kafkaDeleteTopicCmd(name string) error {
	return kafkaTopicsCmd("--delete", "--topic", name)
}

func kafkaCreateTopicCmd(name string) error {
	return kafkaTopicsCmd("--create", "--topic", name, "--partitions", "1", "--replication-factor", "1")
}

func kafkaDeleteConsumerGroup(name string) error {
	return kafkaConsumerGroupsCmd("--delete", "--group", name)
}

func kafkaTopicsCmd(args ...string) error {
	return kafkaCmd("kafka-topics", args...)
}

func kafkaConsumerGroupsCmd(args ...string) error {
	return kafkaCmd("kafka-consumer-groups", args...)
}

func kafkaCmd(cmdName string, args ...string) error {
	args = append(args, "--zookeeper", "localhost:2181")
	out, err := exec.Command(cmdName, args...).Output()

	if err != nil {
		cmd := []string{cmdName}
		cmd = append(cmd, args...)
		return fmt.Errorf(`unexpected error occurred for command "%s": %s, %s`, strings.Join(cmd, " "), err.Error(), string(out))
	}

	return nil
}

func debug() bool {
	return os.Getenv("DEBUG") == "true"
}