/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.kafka.producer;

import com.hubrick.vertx.kafka.producer.config.KafkaProducerConfiguration;
import com.hubrick.vertx.kafka.producer.model.AbstractKafkaMessage;
import com.hubrick.vertx.kafka.producer.model.ByteKafkaMessage;
import com.hubrick.vertx.kafka.producer.model.KafkaOptions;
import com.hubrick.vertx.kafka.producer.model.StringKafkaMessage;
import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class DefaultKafkaProducerService implements KafkaProducerService {

    private final static Counter MESSAGE_ERRORS = Counter.build()
            .name("kafka_producer_messages_error_total")
            .help("Total invalid messages")
            .register();

    private final static Counter MESSAGE_INVALID = Counter.build()
            .name("kafka_producer_messages_invalid_total")
            .help("Total invalid messages")
            .register();


    private final static Histogram DURATION_HISTOGRAM = Histogram.build()
            .name("kafka_producer_messages_duration_seconds")
            .help("Message processing duration in seconds")
            .labelNames("topic", "state")
            .buckets(0.01D, 0.025D, 0.05D, 0.075D, 0.1D, 0.25D, 0.5D, 0.75D, 1D, 2.5D, 5D, 7.5D, 9.99D)
            .register();


    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaProducerService.class);

    private final KafkaProducerConfiguration kafkaProducerConfiguration;

    private Map<MessageSerializerType, Producer> producers = new HashMap<>();
    private StatsDClient statsDClient;

    public DefaultKafkaProducerService(KafkaProducerConfiguration kafkaProducerConfiguration) {
        this.kafkaProducerConfiguration = kafkaProducerConfiguration;
    }

    @Override
    public void start() {
        statsDClient = createStatsDClient();
    }

    @Override
    public void stop() {
        if (!producers.isEmpty()) {
            for (Producer producer : producers.values()) {
                producer.close();
            }
        }
        if (statsDClient != null) {
            statsDClient.stop();
        }
    }

    @Override
    public void sendString(StringKafkaMessage message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler) {
        if (!isValid(message.getPayload())) {
            log.error("Invalid kafka message provided. Message not sent to kafka...");
            MESSAGE_INVALID.inc();
            sendError(resultHandler, new IllegalStateException(String.format("Invalid kafka message provided. Property [%s] is not set.", AbstractKafkaMessage.PAYLOAD)));
            return;
        }

        send(MessageSerializerType.STRING_SERIALIZER, message.getPayload(), message.getPartKey(), options, resultHandler);
    }

    @Override
    public void sendBytes(ByteKafkaMessage message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler) {
        if (!isValid(message.getPayload())) {
            log.error("Invalid kafka message provided. Message not sent to kafka...");
            MESSAGE_INVALID.inc();
            sendError(resultHandler, new IllegalStateException(String.format("Invalid kafka message provided. Property [%s] is not set.", AbstractKafkaMessage.PAYLOAD)));
            return;
        }

        send(MessageSerializerType.BYTE_SERIALIZER, message.getPayload().getBytes(), message.getPartKey(), options, resultHandler);
    }

    private void send(MessageSerializerType messageSerializerType, Object payload, String partKey, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler) {
        try {
            long startTime = System.currentTimeMillis();
            final String topic = isValid(options.getTopic()) ? options.getTopic() : kafkaProducerConfiguration.getDefaultTopic();

            final Producer producer = getOrCreateProducer(messageSerializerType);
            producer.send(new ProducerRecord(topic, partKey, payload), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to Kafka broker...", exception);
                    DURATION_HISTOGRAM.labels(topic, "error").observe(System.currentTimeMillis() - startTime / 1000D);
                    sendError(resultHandler, exception);
                }

                if (metadata != null) {
                    statsDClient.recordExecutionTime("submitted", (System.currentTimeMillis() - startTime));

                    sendOK(resultHandler);
                    DURATION_HISTOGRAM.labels(topic, "success").observe(System.currentTimeMillis() - startTime / 1000D);
                    log.info("Message sent to kafka topic: {}. Payload: {}", topic, payload);
                }
            } );
        } catch (Throwable t) {
            log.error("Failed to send message to Kafka broker...", t);
            sendError(resultHandler, t);
            MESSAGE_ERRORS.inc();
        }
    }

    /**
     * Returns an initialized instance of kafka producer.
     *
     * @return initialized kafka producer
     */
    private Producer getOrCreateProducer(MessageSerializerType messageSerializerType) {
        if (producers.get(messageSerializerType) == null) {
            final Map<String,Object> props = new HashMap<>();
            props.put("producer.type", kafkaProducerConfiguration.getType().getValue());
            props.put("message.send.max.retries", kafkaProducerConfiguration.getMaxRetries().toString());
            props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaProducerConfiguration.getRetryBackoffMs().toString());
            props.put("queue.buffering.max.ms", kafkaProducerConfiguration.getBufferingMaxMs().toString());
            props.put("queue.buffering.max.messages", kafkaProducerConfiguration.getBufferingMaxMessages().toString());
            props.put("queue.enqueue.timeout.ms", kafkaProducerConfiguration.getEnqueueTimeout().toString());
            props.put("batch.num.messages", kafkaProducerConfiguration.getBatchMessageNum().toString());
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfiguration.getBootstrapServers());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, messageSerializerType.getValue());
            props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(kafkaProducerConfiguration.getAcks()));
            props.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerConfiguration.getMaxRetries());
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerConfiguration.getRequestTimeoutMs());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // always use String serializer for the key
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, kafkaProducerConfiguration.getMaxBlockTimeMs());
            props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, kafkaProducerConfiguration.getMetricConsumerClasses());
            props.put("metric.dropwizard.registry", String.valueOf(kafkaProducerConfiguration.getMetricDropwizardRegistryName()));

            producers.put(messageSerializerType, new KafkaProducer(props));
        }

        return producers.get(messageSerializerType);
    }

    /**
     * Returns an initialized instance of the StatsDClient If StatsD is enabled
     * this is a NonBlockingStatsDClient which guarantees not to block the thread or 
     * throw exceptions.   If StatsD is not enabled it creates a NoOpStatsDClient which 
     * contains all empty methods
     *
     * @return initialized StatsDClient
     */
    protected StatsDClient createStatsDClient() {

        if (kafkaProducerConfiguration.getStatsDConfiguration() != null) {
            final String prefix = kafkaProducerConfiguration.getStatsDConfiguration().getPrefix();
            final String host = kafkaProducerConfiguration.getStatsDConfiguration().getHost();
            final int port = kafkaProducerConfiguration.getStatsDConfiguration().getPort();
            return new NonBlockingStatsDClient(prefix, host, port);
        } else {
            return new NoOpStatsDClient();
        }
    }

    private boolean isValid(String str) {
        return str != null && !str.isEmpty();
    }

    private boolean isValid(Buffer buffer) {
        return buffer != null && buffer.length() > 0;
    }

    protected void sendOK(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    protected void sendError(Handler<AsyncResult<Void>> resultHandler, Throwable t) {
        resultHandler.handle(Future.failedFuture(t));
    }
}
