### kafka-proto-consumer-apis


kafka-proto-consumer-apis provides kafka consumer api which reads protobuf encoded messages from a kafka topic with unique identifier you want to search and message is returned as response body.

This utility can be useful when you want to quickly look into the protobuf messages of a topic and search specific key value pair out of random messages on kafka topic. It works over the proto source file and don't need you to compile it using protoc.


### Build 
```
from source 
$ cd $GOPATH/src 
$ git clone https://github.com/bhambri94/kafka-proto-consumer-api.git 
$ GO111MODULE=on go build -o ./kafka-proto-consumer-api main.go 
$ ./kafka-proto-consumer-api
```

### Usage

#### Consumer Api: 

```
curl -X GET
http://localhost:3001/kafka-consumer
-H 'Content-Type: application/json'
-H 'Postman-Token: 29e21c33-4269-439d-bbe6-9c214f0a2b71'
-H 'cache-control: no-cache'
-d '{ "kafkaBroker":"localhost:9092", "topic":"sample_event_topic", "protoPath":"/Users/shivamkumar/go/src/github.com/kafka-proto-consumer-api/protoUtil", "protoFileName":"Sample.proto", "protoMessageName":"protoUtil.Sample", "uniqueIdentifier":"Id", "uniqueIdentifierValue":"Aut_955", "pollIntervalSeconds":"100" }'
```

Description kafkaBroker - kafka broker from where you want to consume kafka message.
topic - Kafka topic 
protoPath - path to the proto file 
protoFileName - proto file name 
protoMessageName - message name from proto file you are expecting in the topic mentioned 
uniqueIdentifier - the key field name that is unique in each proto message 
uniqueIdentifierValue - unique identifier value 
pollIntervalSeconds - time in seconds you want to poll at mentioned topic

#### Producer Api to test locally: 
```
curl -X GET
http://localhost:3001/kafka-producer/topic=sample_event_topic
-H 'Accept: /'
-H 'Accept-Encoding: gzip, deflate'
-H 'Cache-Control: no-cache'
-H 'Connection: keep-alive'
-H 'Content-Length: 107'
-H 'Content-Type: application/json'
-H 'Host: localhost:3001'
-H 'Postman-Token: 81b257e6-5bac-4ab8-bf85-d9883a50b12a,fb757c4f-f534-4ae6-a174-35b20e704f16'
-H 'User-Agent: PostmanRuntime/7.20.1'
-H 'cache-control: no-cache'
-d '{ "Id": "Aut_955", "Label": "DummyLabel", "Description": "DummyDescription", "Version": 1 }'
```
