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
package com.hubrick.vertx.kafka.consumer;

import com.hubrick.vertx.kafka.AbstractVertxTest;
import com.hubrick.vertx.kafka.consumer.property.KafkaConsumerProperties;
import io.vertx.core.json.JsonObject;

public abstract class AbstractConsumerTest extends AbstractVertxTest {

    protected static String SERVICE_NAME = "service:com.hubrick.services.kafka-consumer";

    static JsonObject makeDefaultConfig() {
        JsonObject config = new JsonObject();
        config.put(KafkaConsumerProperties.KEY_VERTX_ADDRESS, "kafkaConsumer");
        config.put(KafkaConsumerProperties.KEY_CLIENT_ID, "kafkaConsumer");
        config.put(KafkaConsumerProperties.KEY_GROUP_ID, "group");
        config.put(KafkaConsumerProperties.KEY_CLIENT_ID, "client");
        config.put(KafkaConsumerProperties.KEY_KAFKA_TOPIC, "topic");
        config.put(KafkaConsumerProperties.KEY_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        return config;
    }

    @Override
    protected String getServiceName() {
        return SERVICE_NAME;
    }
}
