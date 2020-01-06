# kafka-proto-consumer-apis

Producer Api:
curl -X GET \
  http://localhost:3001/kafka-producer/topic=sample_event_topic \
  -H 'Accept: */*' \
  -H 'Accept-Encoding: gzip, deflate' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Content-Length: 107' \
  -H 'Content-Type: application/json' \
  -H 'Host: localhost:3001' \
  -H 'Postman-Token: 81b257e6-5bac-4ab8-bf85-d9883a50b12a,fb757c4f-f534-4ae6-a174-35b20e704f16' \
  -H 'User-Agent: PostmanRuntime/7.20.1' \
  -H 'cache-control: no-cache' \
  -d '{
    "Id": "Aut_955",
    "Label": "DummyLabel",
    "Description": "DummyDescription",
    "Version": 1
}'

Consumer Api:
curl -X GET \
  http://localhost:3001/kafka-consumer \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 29e21c33-4269-439d-bbe6-9c214f0a2b71' \
  -H 'cache-control: no-cache' \
  -d '{
	"kafkaBroker":"localhost:9092",
	"topic":"sample_event_topic",
	"protoPath":"/Users/shivamkumar/go/src/github.com/kafka-proto-consumer-api/protoUtil",
	"protoFileName":"Sample.proto",
	"protoMessageName":"protoUtil.Sample",
	"uniqueIdentifier":"Id",
	"uniqueIdentifierValue":"Aut_955",
	"pollIntervalSeconds":"100"
}'
