package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

func getConfluentLibConfig(brokers []string) *kafka.ConfigMap {
	config := &kafka.ConfigMap{
		"bootstrap.servers":     strings.Join(brokers, ","),
		"broker.address.family": "v4",
		"client.id":             "ksnap",
		"enable.auto.commit":    "false",
		"enable.partition.eof":  "true",
		"check.crcs":            "true",
		"enable.idempotence":    "true",
		"group.id":              "TempKafkaGroup1",
	}

	return config
}

func getSaramaConfig() *sarama.Config {
	// Common settings
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0
	config.ClientID = "ksnap"

	// Consumer settings
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Producer settings
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Admin settings

	return config
}

var OffsetOldest = sarama.OffsetOldest
var OffsetNewest = sarama.OffsetNewest
