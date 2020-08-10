package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
)

func getKafkaConfig(brokers []string) *kafka.ConfigMap {
	config := &kafka.ConfigMap{
		"bootstrap.servers":     strings.Join(brokers, ","),
		"broker.address.family": "v4",
		"client.id":             "ksnap",
		"enable.auto.commit":    "false",
		"enable.partition.eof":  "true",
		"check.crcs":            "true",
		"enable.idempotence":    "true",
	}

	return config
}

var OffsetOldest = kafka.OffsetBeginning
var OffsetNewest = kafka.OffsetEnd
