package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Client interface {
	Topics() []Topic
	Topic(string) Topic
}

type client struct {
	brokers []string
	topics  []Topic
}

func (c *client) Topics() []Topic {
	return c.topics
}

func (c *client) Topic(name string) Topic {
	for _, t := range c.topics {
		if t.Name() == name {
			return t
		}
	}

	return nil
}

func NewClient(brokers []string) (Client, error) {
	var err error

	c := new(client)

	c.brokers = brokers

	consumer, err := kafka.NewConsumer(getKafkaConfig(brokers))
	if err != nil {
		return nil, err
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			panic(err)
		}
	}()

	metadata, err := consumer.GetMetadata(nil, true, 5000)
	if err != nil {
		return nil, err
	}

	for _, topicMetadata := range metadata.Topics {
		t, err := newTopic(brokers, topicMetadata, consumer)
		if err != nil {
			return nil, err
		}

		c.topics = append(c.topics, t)
	}

	return c, nil
}
