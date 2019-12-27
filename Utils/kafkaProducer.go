package Utils

import (
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
)

func CreateKafkaProducer(topic string, msg proto.Message) {
	flag.Parse()

	if brokerList == "" {
		fmt.Println("no -brokers specified.")
	}
	if topic == "" {
		fmt.Println("no -topic specified")
	}

	config := sarama.NewConfig()
	// config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	switch partitioner {
	case "":
		if partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if partition == -1 {
			fmt.Println("-partition is required when partitioning manually")
		}
	default:
		fmt.Println(fmt.Sprintf("Partitioner %s not supported.", partitioner))
	}

	message := &sarama.ProducerMessage{Topic: topic, Partition: int32(partition)}
	producer, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), config)
	if err != nil {
		fmt.Println("Failed to open Kafka producer")
	}
	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	messageBytes, err := proto.Marshal(msg)
	if err != nil {
		fmt.Println("[KafkaClient] Proto marshal error: %s", err.Error())
	}
	fmt.Print("Producing message: ")
	fmt.Println(msg)
	message.Value = sarama.ByteEncoder(messageBytes)

	// go send(producer, message)
	partitionNumber, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Printf("Failed to produce message: %s", err)
	} else {
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partitionNumber, offset)
	}

}
func send(producer sarama.SyncProducer, message *sarama.ProducerMessage) {
	partitionNumber, offset, err := producer.SendMessage(message)
	if err != nil {
		fmt.Printf("Failed to produce message: %s", err)
	} else if !silent {
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", topic, partitionNumber, offset)
	}
}
