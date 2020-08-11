package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/akamensky/go-log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"ksnap/internal/message"
	"time"
)

type Partition interface {
	Topic() string
	Id() int32
	StartOffset() int64
	EndOffset() int64
	Size() int64
	GetConsumerOffsets() (map[string]Offset, error)
	SetConsumerOffsets(map[string]Offset) error
	ReadMessages() <-chan message.Message
	WriteMessages() (chan<- message.Message, <-chan interface{}, <-chan map[int64]int64)
}

type partition struct {
	brokers []string
	topic   string
	id      int32
	start   int64
	end     int64
}

func newPartition(brokers []string, topic string, metadata kafka.PartitionMetadata, consumer *kafka.Consumer) (Partition, error) {
	var err error
	p := new(partition)

	p.brokers = brokers
	p.topic = topic
	p.id = metadata.ID

	// Get topic offsets
	p.start, p.end, err = consumer.GetWatermarkOffsets(p.topic, p.id)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *partition) Topic() string {
	return p.topic
}

func (p *partition) Id() int32 {
	return p.id
}

func (p *partition) StartOffset() int64 {
	return p.start
}

func (p *partition) EndOffset() int64 {
	return p.end
}

func (p *partition) Size() int64 {
	return p.end - p.start
}

func (p *partition) kafkaLibTopicPartition() kafka.TopicPartition {
	return kafka.TopicPartition{
		Topic:     &p.topic,
		Partition: p.id,
	}
}

func (p *partition) GetConsumerOffsets() (map[string]Offset, error) {
	return getConsumerOffsets(p.brokers, p.topic, p.id)
}

func (p *partition) SetConsumerOffsets(offsets map[string]Offset) error {
	for consumerGroup, offset := range offsets {
		err := setConsumerOffsets(p.brokers, p.topic, p.id, consumerGroup, offset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *partition) ReadMessages() <-chan message.Message {
	result := make(chan message.Message)

	go func(p *partition, output chan<- message.Message) {
		defer func() {
			if r := recover(); r != nil {
				log.Fatalf("Error when transforming message from Kafka for topic [%s], partition [%d], the error was: %s", p.topic, p.id, r)
			}
		}()

		c, err := kafka.NewConsumer(getConfluentLibConfig(p.brokers))
		if err != nil {
			panic(err)
		}

		tp := p.kafkaLibTopicPartition()
		tp.Offset, err = kafka.NewOffset(p.StartOffset())
		if err != nil {
			panic(err)
		}

		err = c.Assign([]kafka.TopicPartition{p.kafkaLibTopicPartition()})
		if err != nil {
			panic(err)
		}

		lastOffset := p.EndOffset() - 1

		for true {
			ev := c.Poll(5000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				msgHeaders := make([]message.Header, len(e.Headers))
				for _, kafkaHeader := range e.Headers {
					msgHeaders = append(msgHeaders, message.NewHeader([]byte(kafkaHeader.Key), kafkaHeader.Value))
				}
				msg := message.NewMessage(*e.TopicPartition.Topic, e.TopicPartition.Partition, int64(e.TopicPartition.Offset), e.Key, e.Value, e.Timestamp, time.Now(), msgHeaders)

				output <- msg

				if int64(e.TopicPartition.Offset) >= lastOffset {
					err = c.Close()
					if err != nil {
						panic(err)
					}

					close(output)

					return
				}
			case kafka.Error:
				if e.Code() == kafka.ErrPartitionEOF {
					close(output)
					return
				} else {
					panic(e)
				}
			}
		}
	}(p, result)

	return result
}

func (p *partition) WriteMessages() (chan<- message.Message, <-chan interface{}, <-chan map[int64]int64) {
	msgCh := make(chan message.Message)
	doneCh := make(chan interface{})
	offsetCh := make(chan map[int64]int64)

	sp, err := sarama.NewSyncProducer(p.brokers, getSaramaConfig())
	if err != nil {
		panic(err)
	}

	go func(p *partition, sp sarama.SyncProducer, ch <-chan message.Message, doneCh chan interface{}, offsetCh chan<- map[int64]int64) {
		for ksnapMessage := range ch {
			saramaMessage := &sarama.ProducerMessage{
				Topic:     ksnapMessage.Topic(),
				Key:       sarama.ByteEncoder(ksnapMessage.Key()),
				Value:     sarama.ByteEncoder(ksnapMessage.Value()),
				Headers:   nil,
				Metadata:  nil,
				Offset:    ksnapMessage.Offset(),
				Partition: ksnapMessage.Partition(),
				Timestamp: ksnapMessage.Timestamp(),
			}
			for _, header := range ksnapMessage.Headers() {
				saramaMessage.Headers = append(saramaMessage.Headers, sarama.RecordHeader{
					Key:   header.Key(),
					Value: header.Value(),
				})
			}

			responsePartition, responseOffset, err := sp.SendMessage(saramaMessage)
			if err != nil {
				panic(fmt.Sprintf("Error writing message to topic [%s] partition [%d]. Error was: %s", ksnapMessage.Topic(), ksnapMessage.Partition(), err.Error()))
			}

			// if written to different partition -- that is very bad
			if responsePartition != ksnapMessage.Partition() {
				panic(fmt.Sprintf("Error writing message to topic [%s] partition [%d], message was written to partition [%d] instead", ksnapMessage.Topic(), ksnapMessage.Partition(), responsePartition))
			}

			offsetMap := map[int64]int64{ksnapMessage.Offset(): responseOffset}
			offsetCh <- offsetMap
		}

		// SyncProducer MUST be closed in order to flush all internally cached messages
		err := sp.Close()
		if err != nil {
			panic(err)
		}

		close(doneCh)
		close(offsetCh)
	}(p, sp, msgCh, doneCh, offsetCh)

	return msgCh, doneCh, offsetCh
}
