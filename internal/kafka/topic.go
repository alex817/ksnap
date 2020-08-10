package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Topic interface {
	Name() string
	Partitions() []Partition
}

type topic struct {
	brokers    []string
	name       string
	partitions []Partition
}

func newTopic(brokers []string, metadata kafka.TopicMetadata, consumer *kafka.Consumer) (Topic, error) {
	t := new(topic)
	t.brokers = brokers
	t.name = metadata.Topic

	for _, partitionMetadata := range metadata.Partitions {
		p, err := newPartition(brokers, t.name, partitionMetadata, consumer)
		if err != nil {
			return nil, err
		}

		t.partitions = append(t.partitions, p)
	}

	return t, nil
}

func (t *topic) Name() string {
	return t.name
}

func (t *topic) Partitions() []Partition {
	return t.partitions
}
