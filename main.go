package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/kafka-proto-consumer-api/Utils"
	"github.com/kafka-proto-consumer-api/protoUtil"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
)

type event struct {
	EventState string `json:"EventState"`
	Message    string `json:"Message"`
}

type NewEvent struct {
	Id          string
	Label       string
	Description string
	Version     int32
}

func main() {
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		fmt.Println("Shutting down Server...")
		os.Exit(64)
	}()

	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/kafka-consumer/topic={KafkaTopic}&Id={Id}&PollIntervalSeconds={interval}", consumerPoll)
	router.HandleFunc("/kafka-consumer", KafkaConsumerWithAnyProtoPathAndUniqueIdentifierSearchPoll)
	router.HandleFunc("/kafka-producer/topic={KafkaTopic}", ProducerToTopic)
	fmt.Println("Server started successfully on port 3001")
	log.Fatal(http.ListenAndServe(":3001", router))
}

func consumerPoll(w http.ResponseWriter, r *http.Request) {
	topic := mux.Vars(r)["KafkaTopic"]
	DriverID := mux.Vars(r)["Id"]
	PollIntervalSeconds := mux.Vars(r)["interval"]
	seconds, _ := strconv.Atoi(PollIntervalSeconds)
	reqBody, err := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")

	a, b := Utils.CreateKafkaConsumer(topic, DriverID, seconds, "Sample")
	if err != nil {
		fmt.Fprintf(w, "error")
	}
	if a == "expired" {
		b = ""
	}
	newEvent := event{
		EventState: a,
		Message:    b,
	}

	json.Unmarshal(reqBody, &newEvent)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newEvent)
}

type KafkaConsumerRequest struct {
	Topic                 string `json:"topic"`
	ProtoPath             string `json:"protoPath"`
	ProtoMessageName      string `json:"protoMessageName"`
	ProtoFileName         string `json:"protoFileName"`
	UniqueIdentifier      string `json:"uniqueIdentifier"`
	UniqueIdentifierValue string `json:"uniqueIdentifierValue"`
	PollIntervalSeconds   string `json:"pollIntervalSeconds"`
}

func KafkaConsumerWithAnyProtoPathAndUniqueIdentifierSearchPoll(w http.ResponseWriter, r *http.Request) {
	var kafkaConsumerRequest KafkaConsumerRequest

	w.Header().Set("Content-Type", "application/json")

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&kafkaConsumerRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		// return
	}

	topic := kafkaConsumerRequest.Topic
	UniqueIdentifier := kafkaConsumerRequest.UniqueIdentifier
	UniqueIdentifierValue := kafkaConsumerRequest.UniqueIdentifierValue
	PollIntervalSeconds := kafkaConsumerRequest.PollIntervalSeconds
	ProtoPath := []string{kafkaConsumerRequest.ProtoPath}
	ProtoFileName := kafkaConsumerRequest.ProtoFileName
	ProtoMessageName := kafkaConsumerRequest.ProtoMessageName
	seconds, _ := strconv.Atoi(PollIntervalSeconds)
	reqBody, err := ioutil.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	fmt.Println(topic, UniqueIdentifier, UniqueIdentifierValue, PollIntervalSeconds, ProtoFileName, ProtoMessageName)

	protoDetails := Utils.ProtobufDetails{
		ProtoPath:             ProtoPath,
		ProtoFileName:         ProtoFileName,
		ProtoMessage:          ProtoMessageName,
		UniqueIdentifier:      UniqueIdentifier,
		UniqueIdentifierValue: UniqueIdentifierValue,
	}

	a, b := Utils.CreateKafkaConsumerWithVariableProto(topic, protoDetails, seconds)
	if err != nil {
		fmt.Fprintf(w, "error")
	}
	if a == "expired" {
		b = ""
	}
	newEvent := event{
		EventState: a,
		Message:    b,
	}

	json.Unmarshal(reqBody, &newEvent)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(newEvent)
}

func ProducerToTopic(w http.ResponseWriter, r *http.Request) {
	var e NewEvent
	topic := mux.Vars(r)["KafkaTopic"]
	w.Header().Set("Content-Type", "application/json")

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&e)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		// return
	}

	b, err := json.Marshal(e)
	if err != nil {
		fmt.Println(err)
	}

	protoMessage := &protoUtil.Sample{}
	// JSON to Proto
	if err := jsonpb.Unmarshal(strings.NewReader(string(b)), protoMessage); err != nil {
		log.Fatalln("Error converting JSON to proto:", err)
	}

	fmt.Println("Producing message with Id: " + e.Id)
	Utils.CreateKafkaProducer(topic, protoMessage)
	w.WriteHeader(http.StatusCreated)
}

func GetNewEvent() (proto.Message, string) {

	// s1 := rand.NewSource(time.Now().UnixNano())
	// r1 := rand.New(s1)
	// event := "Aut_" + strconv.Itoa(r1.Intn(100000000))

	event := "Aut_954"
	JsonEvent := &NewEvent{
		Id:          event,
		Label:       "DummyLabel",
		Description: "DummyDescription",
		Version:     1,
	}
	b, err := json.Marshal(JsonEvent)
	if err != nil {
		fmt.Println(err)
	}

	protoMessage := &protoUtil.Sample{}
	// JSON to Proto
	if err := jsonpb.Unmarshal(strings.NewReader(string(b)), protoMessage); err != nil {
		log.Fatalln("Error converting JSON to proto:", err)
	}

	return protoMessage, event
}
