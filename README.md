# Vert.x Kafka Service

## Compatibility
- Java 8+
- Vert.x 3.1.0 >=

 Vert.x version     | Library version
 ------------------ | ----------------
 3.1.0              | 1.0.0
 3.3.0              | 1.1.0

## Vert.x Kafka Consumer
This service allows you to bridge messages from Kafka to the Vert.x Event Bus. It allows asynchronous message processing while still maintaining a correct Kafka offset.

It achieves "at-least-once" semantics, all Event Bus messages must be acknowledged by the handler in order to commit the current Kafka offset. This means that your handler on the Event Bus must be able to handle message replays.

When certain limits are reached a commit cycle will happen. A commit cycle waits for all outstanding acknowledgements in order to commit the current Kafka offset. 

Commit cycles will happen on any of the following conditions:

 * `maxUnacknowledged` is reached, meaning that this amount of messages is currently unacknowledged by the Vert.x handlers.
 * `maxUncommited` is reached, meaning that the difference between the last offset that was committed and the current offset is `maxUncommited`
 * The Kafka partition from which the consumer consumes is switched. In order to reduce the amount of commit cycles caused by this condition one should start a consumer per partition.
 
## Vert.x Kafka Producer
This service allows to receive events published by other Vert.x verticles and send those events to Kafka broker.

## Dependencies

### Maven
```xml
<dependency>
    <groupId>com.hubrick.vertx</groupId>
    <artifactId>vertx-kafka-service</artifactId>
    <version>1.1.0</version>
</dependency>
```

## How to use

### Configuration:
### Consumer:

Service id: com.hubrick.services.kafka-consumer

```JSON
    {
      "address" : "message-from-kafka",
      "groupId" : "groupId",
      "kafkaTopic" : "kafka-topic",
      "zk" : "host:port",
      "offsetReset" : "largest",
      "maxUnacknowledged" : 100,
      "maxUncommitted" : 1000,
      "ackTimeoutSeconds" : 600,
      "maxRetries" : 100,
      "initialRetryDelaySeconds" : 1,
      "maxRetryDelaySeconds" : 10,
      "eventBusSendTimeout" : 30
    }
```

* `address`: Vert.x event bus address the Kafka messages are relayed to (Required)
* `groupId`: Kafka Group Id to use for the Kafka consumer (Required)
* `kafkaTopic`: The Kafka topic to subscribe to (Required)
* `zk`: Zookeeper host and port (Required)
* `offsetReset`: What to do when there is no initial offset in ZooKeeper or if an offset is out of range (Default: largest)
* `maxUnacknowledged`: how many messages from Kafka can be unacknowledged before the module waits for all missing acknowledgements, effectively limiting the amount of messages that are on the Vertx Event Bus at any given time. (Default: 100)
* `maxUncommitted`: max offset difference before a commit cycle is run. A commit cycle waits for all unacknowledged messages and then commits the offset to Kafka. Note that when you read from multiple partitions the offset is not continuous and therefore every partition switch causes a commit cycle. For better performance you should start an instance of the module per partition. (Default: 1000)
* `ackTimeoutSeconds`: the time to wait for all outstanding acknowledgements during a commit cycle. This will just lead to a log message saying how many ACKs are still missing, as the module will wait forever for ACKs in order to achieve at least once semantics. (Default: 600)
* `maxRetries`: Max number of retries until it consider the message failed (Default: infinite)
* `initialRetryDelaySeconds`: Initial retry delay (Default: 1)
* `maxRetryDelaySeconds`: Max retry delay since the retry delay is increasing (Default: 10)
* `eventBusSendTimeout: the send timeout for the messages that are relayed to the Vertx Event Bus. That is the time the handler has to handle and respond to the message.`

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
      "brokerList" : "localhost:9092",          
      "requiredAcks" : 1,
      "statsD" : {
        "prefix" : "vertx.kafka",                
        "host" : "localhost",                   
        "port" : 8125                            
      }
    }
```

* `address`: Vert.x event bus address (Required)
* `defaultTopic`: Topic used if no other specified during sending (Required)
* `brokerList`: The Kafka broker list (Default: localhost:9092)
* `requiredAcks`: The minimum number of required acks to acknowledge the sending (Default: 1)
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
