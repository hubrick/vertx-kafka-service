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

import com.hubrick.vertx.kafka.producer.model.ByteKafkaMessage;
import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class ManualTest {

    public static void main(String[] args) {
        JsonObject config = new JsonObject();
        config.put(KafkaProducerProperties.ADDRESS, "test-address");
        config.put(KafkaProducerProperties.BOOTSTRAP_SERVERS, KafkaProducerProperties.BOOTSTRAP_SERVERS_DEFAULT);
        config.put(KafkaProducerProperties.DEFAULT_TOPIC, "test-topic");
        config.put(KafkaProducerProperties.ACKS, KafkaProducerProperties.ACKS_DEFAULT);

        final Vertx vertx = Vertx.vertx();
        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        vertx.deployVerticle("service:com.hubrick.services.kafka-producer", deploymentOptions, result -> {
            System.out.println(result.result());

            JsonObject jsonObject = new JsonObject();
            jsonObject.put("payload", "your message goes here".getBytes());

            final KafkaProducerService kafkaProducerService = KafkaProducerService.createProxy(vertx, "test-address");
            kafkaProducerService.sendBytes(new ByteKafkaMessage(Buffer.buffer("your message goes here".getBytes()), "test-partition"), response -> {
                if (response.succeeded()) {
                    System.out.println("OK");
                } else {
                    System.out.println("Failed");
                    response.cause().printStackTrace();
                }

                vertx.close();
            });
        });
    }
}
