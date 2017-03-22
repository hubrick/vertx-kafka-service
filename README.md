# Vert.x Kafka Service

## Compatibility
- Java 8+
- Vert.x 3.1.0 >=

 Vert.x version     | Kafka Version     | Library version
 ------------------ | ----------------- | ----------------
 3.1.0              | 0.8.x             | 1.0.0
 3.3.0              | 0.8.x             | 1.1.0
 3.3.3              | 0.10.1            | 1.2.0
  

## Vert.x Kafka Consumer
This service allows you to bridge messages from Kafka to the Vert.x Event Bus. It allows asynchronous message processing while still maintaining a correct Kafka offset.

It achieves "at-least-once" semantics, all Event Bus messages must be acknowledged by the handler in order to commit the current Kafka offset. This means that your handler on the Event Bus must be able to handle message replays.

The ordering is in so far preserved as the messages are relayed to the Vert.x EventBus in the order of arrival. If certain actions take longer than others the order will get violated, because the consumer does not wait for the ACKs before relaying the next message. To ensure strict ordering enable `strictOrdering`.

When certain limits are reached a commit cycle will happen. A commit cycle waits for all outstanding acknowledgements in order to commit the current Kafka offset. 

Commit cycles will happen on any of the following conditions:

 * `maxUnacknowledged` is reached, meaning that this amount of messages is currently unacknowledged by the Vert.x handlers.
 * `maxUncommited` is reached, meaning that the difference between the last offset that was committed and the current offset is `maxUncommited`
 * The Kafka partition from which the consumer consumes is switched. In order to reduce the amount of commit cycles caused by this condition one should start a consumer per partition or disable this via `commitOnPartitionChange`.
 * `commitTimeoutMs` is reached, meaning the time between the last and the current message was bigger than last commit timeout in milliseconds. 
 
## Vert.x Kafka Producer
This service allows to receive events published by other Vert.x verticles and send those events to Kafka broker.

## Dependencies

### Maven
```xml
<dependency>
    <groupId>com.hubrick.vertx</groupId>
    <artifactId>vertx-kafka-service</artifactId>
    <version>1.2.0</version>
</dependency>
```

## How to use

### Configuration:
### Consumer:

Service id: com.hubrick.services.kafka-consumer

```JSON
    {
      "address" : "message-from-kafka",
      "clientId" : "clientId", 
      "groupId" : "groupId",
      "kafkaTopic" : "kafka-topic",
      "bootstrapServers" : "kafka-server:9092",
      "offsetReset" : "largest",
      "maxUnacknowledged" : 100,
      "maxUncommitted" : 1000,
      "ackTimeoutSeconds" : 600,
      "commitTimeoutMs" : 300000, 
      "maxRetries" : 100,
      "initialRetryDelaySeconds" : 1,
      "maxRetryDelaySeconds" : 10,
      "eventBusSendTimeout" : 30,
      "messagesPerSecond" : -1.0,
      "commitOnPartitionChange": true,
      "strictOrdering": false
    }
```

* `address`: Vert.x event bus address the Kafka messages are relayed to (required)
* `clientId`: Kafka client id to use for the Kafka consumer (required)
* `groupId`: Kafka Group Id to use for the Kafka consumer (required)
* `kafkaTopic`: The Kafka topic to subscribe to (required)
* `bootstrapServers`: Kafka servers host and port to connect to (required)
* `offsetReset`: What to do when there is no initial offset in ZooKeeper or if an offset is out of range (Default: largest)
* `maxUnacknowledged`: how many messages from Kafka can be unacknowledged before the module waits for all missing acknowledgements, effectively limiting the amount of messages that are on the Vertx Event Bus at any given time. (Default: 100)
* `maxUncommitted`: max offset difference before a commit cycle is run. A commit cycle waits for all unacknowledged messages and then commits the offset to Kafka. Note that when you read from multiple partitions the offset is not continuous and therefore every partition switch causes a commit cycle. For better performance you should start an instance of the module per partition. (Default: 1000)
* `ackTimeoutSeconds`: the time to wait for all outstanding acknowledgements during a commit cycle. This will just lead to a log message saying how many ACKs are still missing, as the module will wait forever for ACKs in order to achieve at least once semantics. (Default: 600)
* `commitTimeoutMs`: the time to wait to force a commit cycle in case there is not a lot of traffic on your topic. It will check if between and the last message the timeout has been passed and will commit if so. 
* `maxRetries`: Max number of retries until it consider the message failed (Default: infinite)
* `initialRetryDelaySeconds`: Initial retry delay (Default: 1)
* `maxRetryDelaySeconds`: Max retry delay since the retry delay is increasing (Default: 10)
* `eventBusSendTimeout`: the send timeout for the messages that are relayed to the Vertx Event Bus. That is the time the handler has to handle and respond to the message.`
* `messagesPerSecond`: the number of messages that should be relayed per second (Double, values bigger than 0.0 will limit, everything else is unlimitted)
* `commitOnPartitionChange`: Run a commit cycle when the partition changes. This is mostly another trigger if you do not have that many messages on a topic and want to make sure a commit happens regularly. (Default: true)
* `strictOrdering`: Makes the consumer await an acknowledgement before relaying the next message. Messages will thus not be handled in parallel anymore but strictly in the order of arrival. (Default: false)

### Example:

```Java
        vertx.eventBus().registerHandler("message-from-kafka", message -> {
            System.out.println("Got a message from Kafka: " + message.body() );
            message.reply(); // Acknowledge to the Kafka Module that the message has been handled
        });
```


### Producer:
Service id: com.hubrick.services.kafka-producer

```JSON
    {
      "address" : "eventbus-address",        
      "defaultTopic" : "default-topic", 
      "bootstrapServers" : "kafka-server:9092",          
      "acks" : 1,
      "type" : "async",
      "maxRetries" : 3,
      "retryBackoffMs" : 100,
      "bufferingMaxMs" : 5000,
      "bufferingMaxMessages" : 10000,
      "enqueueTimeout" : -1,
      "batchMessageNum" : 200,
      "statsD" : {
        "prefix" : "vertx.kafka",                
        "host" : "localhost",                   
        "port" : 8125                            
      }
    }
```

* `address`: Vert.x event bus address (Required)
* `defaultTopic`: Topic used if no other specified during sending (Required)
* `bootstrapServers`: The Kafka broker list (Default: localhost:9092)
* `acks`: The minimum number of required acks to acknowledge the sending (Default: 1)
* `type`: This parameter specifies whether the messages are sent asynchronously in a background thread (Default: sync)
* `maxRetries`: This property will cause the producer to automatically retry a failed send request. This property specifies the number of retries when such failures occur (Default: 3)
* `retryBackoffMs`: Time to wait before each retry (Default: 100)
* `bufferingMaxMs`: Maximum time to buffer data when using async mode. (Default: 5000)
* `bufferingMaxMessages`: The maximum number of unsent messages that can be queued up the producer when using async mode before either the producer must be blocked or data must be dropped (Default: 10000)
* `enqueueTimeout`: The amount of time to block before dropping messages when running in async mode and the buffer has reached bufferingMaxMessages (Default: -1)
* `batchMessageNum`: The number of messages to send in one batch when using async mode (Default: 200)
* `statsD.prefix`: statsD prefix (Default: vertx.kafka)
* `statsD.host`: statsD host (Default: localhost)
* `statsD.port`: statsD port (Default: 8125)

### Example:

```Java
    final KafkaProducerService kafkaProducerService = KafkaProducerService.createProxy(vertx, "eventbus-address");
    kafkaProducerService.sendString(new StringKafkaMessage("your message goes here", "optional-partition"), new KafkaOptions().setTopic("topic")), response -> {
        if (response.succeeded()) {
            System.out.println("OK");
        } else {
            System.out.println("FAILED");
        }
    });
```


## License
Apache License, Version 2.0
