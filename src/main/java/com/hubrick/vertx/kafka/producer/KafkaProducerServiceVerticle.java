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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.hubrick.vertx.kafka.producer.config.KafkaProducerConfiguration;
import com.hubrick.vertx.kafka.producer.config.ProducerType;
import com.hubrick.vertx.kafka.producer.config.StatsDConfiguration;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ProxyHelper;

import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.ACKS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.ACKS_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.ADDRESS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BATCH_MESSAGE_NUM;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BOOTSTRAP_SERVERS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BOOTSTRAP_SERVERS_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BUFFERING_MAX_MESSAGES;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BUFFERING_MAX_MS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.DEFAULT_TOPIC;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.ENQUEUE_TIMEOUT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.MAX_BLOCK_MS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.MAX_BLOCK_MS_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.MAX_RETRIES;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.METRIC_CONSUMER_CLASSES;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.METRIC_DROPWIZARD_REGISTRY_NAME;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.REQUEST_TIMEOUT_MS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.REQUEST_TIMEOUT_MS_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.RETRIES;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.RETRIES_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.RETRY_BACKOFF_MS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.STATSD;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.TYPE;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.HOST;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.HOST_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PORT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PORT_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PREFIX;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PREFIX_DEFAULT;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class KafkaProducerServiceVerticle extends AbstractVerticle {

    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private KafkaProducerService kafkaProducerService;

    @Override
    public void start() {
        // Get the address of EventBus where the message was published
        final String address = config().getString(ADDRESS);
        if(Strings.isNullOrEmpty(address)) {
            throw new IllegalStateException("address must be specified in config");
        }

        // Get the address of EventBus where the message was published
        final String topic = config().getString(DEFAULT_TOPIC);
        if(Strings.isNullOrEmpty(topic)) {
            throw new IllegalStateException("topic must be specified in config");
        }

        final JsonObject statsDConfig = config().getJsonObject(STATSD);

        StatsDConfiguration statsDConfiguration = null;
        if (statsDConfig != null) {
            final String prefix = statsDConfig.getString(PREFIX, PREFIX_DEFAULT);
            final String host = statsDConfig.getString(HOST, HOST_DEFAULT);
            final int port = statsDConfig.getInteger(PORT, PORT_DEFAULT);
            statsDConfiguration = new StatsDConfiguration(host, port, prefix);
        }

        final KafkaProducerConfiguration kafkaProducerConfiguration = new KafkaProducerConfiguration(
                topic,
                config().getString(BOOTSTRAP_SERVERS, BOOTSTRAP_SERVERS_DEFAULT),
                config().getString(ACKS, ACKS_DEFAULT),
                config().getInteger(RETRIES, RETRIES_DEFAULT),
                config().getInteger(REQUEST_TIMEOUT_MS, REQUEST_TIMEOUT_MS_DEFAULT),
                config().getInteger(MAX_BLOCK_MS, MAX_BLOCK_MS_DEFAULT),
                COMMA_LIST_SPLITTER.splitToList(config().getString(METRIC_CONSUMER_CLASSES, "")),
                config().getString(METRIC_DROPWIZARD_REGISTRY_NAME));
        kafkaProducerConfiguration.setStatsDConfiguration(statsDConfiguration);

        final String type = config().getString(TYPE);
        if(!Strings.isNullOrEmpty(type)) {
            kafkaProducerConfiguration.setType(ProducerType.valueOf(type));
        }

        final Integer maxRetries = config().getInteger(MAX_RETRIES);
        if(maxRetries != null) {
            kafkaProducerConfiguration.setMaxRetries(maxRetries);
        }

        final Integer retryBackoffMs = config().getInteger(RETRY_BACKOFF_MS);
        if(retryBackoffMs != null) {
            kafkaProducerConfiguration.setRetryBackoffMs(retryBackoffMs);
        }

        final Integer bufferingMaxMs = config().getInteger(BUFFERING_MAX_MS);
        if(bufferingMaxMs != null) {
            kafkaProducerConfiguration.setBufferingMaxMs(bufferingMaxMs);
        }

        final Integer bufferingMaxMessages = config().getInteger(BUFFERING_MAX_MESSAGES);
        if(bufferingMaxMessages != null) {
            kafkaProducerConfiguration.setBufferingMaxMessages(bufferingMaxMessages);
        }

        final Integer enqueueTimeout = config().getInteger(ENQUEUE_TIMEOUT);
        if(enqueueTimeout != null) {
            kafkaProducerConfiguration.setEnqueueTimeout(enqueueTimeout);
        }

        final Integer batchMessageNum = config().getInteger(BATCH_MESSAGE_NUM);
        if(batchMessageNum != null) {
            kafkaProducerConfiguration.setBatchMessageNum(batchMessageNum);
        }

        kafkaProducerService = new DefaultKafkaProducerService(kafkaProducerConfiguration);
        ProxyHelper.registerService(KafkaProducerService.class, vertx, kafkaProducerService, address);

        kafkaProducerService.start();
    }

    @Override
    public void stop() {
        if (kafkaProducerService != null) {
            kafkaProducerService.stop();
        }
    }
}
