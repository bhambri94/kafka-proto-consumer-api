package Utils

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kafka-proto-consumer-api/constants"
	"github.com/kafka-proto-consumer-api/protoUtil"
	"github.com/kafka-proto-consumer-api/protobuf_decoder"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
)

var (
	brokerList        = constants.Brokers
	topic             = constants.ConsumeFromTopic
	partitioner       = ""
	partition         = -1
	silent            = true
	partitions        = "all"
	offset            = constants.OffsetStart
	verbose           = false
	bufferSize        = 256
	ReturnBackState   string
	ReturnPayloadData string
)

func CreateKafkaConsumer(topic string, DriverId string, consumeForSeconds int, protoToBeConsumed string) (string, string) {
	ReturnBackState = "false"
	flag.Parse()
	start := time.Now()
	done := make(chan bool)

	fmt.Println("Consuming events from Topic: " + topic + " for Id: " + DriverId + " polling for " + strconv.Itoa(consumeForSeconds) + " seconds")

	if brokerList == "" {
		PrintUsageErrorAndExit("You have to provide -brokers as a comma-separated list")
	}

	if topic == "" {
		PrintUsageErrorAndExit("-topic is required")
	}

	var initialOffset int64
	switch offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		PrintUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(strings.Split(brokerList, ","), nil)
	if err != nil {
		PrintErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := GetPartitions(c)
	if err != nil {
		PrintErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			PrintErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for {
			// fmt.Println(time.Since(start).Seconds())
			if int(time.Since(start).Seconds()) > consumeForSeconds {
				ReturnBackState = "expired"
				done <- true

				break
			}
			time.Sleep(2 * time.Second)
		}

	}()

	if protoToBeConsumed == "Sample" {
		go func() {
			for msg := range messages {
				newMessage := &protoUtil.Sample{}
				err = proto.Unmarshal(msg.Value, newMessage)
				if err != nil {
					fmt.Println("unmarshaling error: ", err)
				}
				if newMessage.Id == DriverId {
					m := &jsonpb.Marshaler{}
					s, err1 := m.MarshalToString(newMessage)
					if err1 != nil {
						log.Println(err)
					}
					fmt.Println(s)
					fmt.Println("MATCHED!! ")
					ReturnBackState = "true"
					ReturnPayloadData = s
					done <- true
				}
			}
		}()
	}

	<-done
	fmt.Println("Done consuming topic", topic)
	close(closing)
	fmt.Println("Initiating shutdown of consumer...")
	if err := c.Close(); err != nil {
		fmt.Println("Failed to close consumer: ", err)
	}

	return ReturnBackState, ReturnPayloadData
}

func GetPartitions(c sarama.Consumer) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

type ProtobufDetails struct {
	ProtoPath             []string `json:"protoPath"`
	ProtoFileName         string   `json:"protoFileName"`
	ProtoMessage          string   `json:"protoMessage"`
	UniqueIdentifier      string   `json:"uniqueIdentifier"`
	UniqueIdentifierValue string   `json:"uniqueIdentifierValue"`
}

func CreateKafkaConsumerWithVariableProto(brokerList string, topic string, ProtoDetails ProtobufDetails, consumeForSeconds int) (string, string) {
	ReturnBackState = "false"
	flag.Parse()
	start := time.Now()
	done := make(chan bool)

	fmt.Println("Consuming events from Topic: " + topic + " for Id: " + ProtoDetails.UniqueIdentifierValue + " polling for " + strconv.Itoa(consumeForSeconds) + " seconds")

	if brokerList == "" {
		PrintUsageErrorAndExit("You have to provide -brokers as a comma-separated list")
	}

	if topic == "" {
		PrintUsageErrorAndExit("-topic is required")
	}

	var initialOffset int64
	switch offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		PrintUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	c, err := sarama.NewConsumer(strings.Split(brokerList, ","), nil)
	if err != nil {
		PrintErrorAndExit(69, "Failed to start consumer: %s", err)
	}

	partitionList, err := GetPartitions(c)
	if err != nil {
		PrintErrorAndExit(69, "Failed to get the list of partitions: %s", err)
	}

	var (
		messages = make(chan *sarama.ConsumerMessage, bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			PrintErrorAndExit(69, "Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}

	go func() {
		for {
			// fmt.Println(time.Since(start).Seconds())
			if int(time.Since(start).Seconds()) > consumeForSeconds {
				ReturnBackState = "expired"
				done <- true

				break
			}
			time.Sleep(2 * time.Second)
		}

	}()

	go func() {
		for msg := range messages {
			stringify, _ := protobuf_decoder.NewProtobufJSONStringify(ProtoDetails.ProtoPath, ProtoDetails.ProtoFileName, ProtoDetails.ProtoMessage)
			findElement, _ := stringify.FieldValue(msg.Value, ProtoDetails.UniqueIdentifier)
			if findElement == ProtoDetails.UniqueIdentifierValue {
				jsonString, _ := stringify.JsonString(msg.Value, false)
				fmt.Println(jsonString)
				fmt.Println("MATCHED!! ")
				ReturnBackState = "true"
				ReturnPayloadData = jsonString
				done <- true
			}
		}
	}()

	<-done
	fmt.Println("Done consuming topic", topic)
	close(closing)
	fmt.Println("Initiating shutdown of consumer...")
	if err := c.Close(); err != nil {
		fmt.Println("Failed to close consumer: ", err)
	}

	return ReturnBackState, ReturnPayloadData
}

func PrintErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func PrintUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}
