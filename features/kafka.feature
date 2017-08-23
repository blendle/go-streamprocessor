@kafka
Feature: Correctly process Kafka messages

  Background: Prepare Kafka
    Given the topic "process-test" exists

  Scenario: Consume a single message
    Given the "hello world" message exists in topic "process-test"
    When the kafka consumer consumes from the "process-test" topic
    Then 1 message should have been consumed

  Scenario: Consume a large amount of messages
    Given 100000 messages exist in topic "process-test"
    When the kafka consumer consumes from the "process-test" topic
    Then 100000 messages should have been consumed

  Scenario: Consume messages in multiple chunks
    Given messages are continuously streamed into the "process-test" topic
    And the kafka consumer consumes from the "process-test" topic
    When the kafka consumer closes after 2 seconds
    And the kafka consumer consumes from the "process-test" topic
    And the kafka consumer closes after 1 second
    And the kafka consumer consumes from the "process-test" topic
    And no more messages are streamed into the "process-test" topic
    Then all messages should have been consumed
