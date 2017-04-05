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

import com.google.common.base.Strings;
import com.hubrick.vertx.kafka.consumer.config.KafkaConsumerConfiguration;
import com.hubrick.vertx.kafka.consumer.property.KafkaConsumerProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;

/**
 * Vert.x Module to read from a Kafka Topic.
 *
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public class KafkaConsumerVerticle extends AbstractVerticle {

    private KafkaConsumerManager consumer;
    private KafkaConsumerConfiguration configuration;
    private String vertxAddress;

    @Override
    public void start() throws Exception {
        super.start();

        final JsonObject config = vertx.getOrCreateContext().config();
        vertxAddress = getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_VERTX_ADDRESS);

        configuration = KafkaConsumerConfiguration.create(
                getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_GROUP_ID),
                getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_CLIENT_ID),
                getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_KAFKA_TOPIC),
                getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_BOOTSTRAP_SERVERS),
                config.getString(KafkaConsumerProperties.KEY_OFFSET_RESET, "latest"),
                config.getInteger(KafkaConsumerProperties.KEY_MAX_UNACKNOWLEDGED, 100),
                config.getLong(KafkaConsumerProperties.KEY_MAX_UNCOMMITTED_OFFSETS, 1000L),
                config.getLong(KafkaConsumerProperties.KEY_ACK_TIMEOUT_SECONDS, 240L),
                config.getLong(KafkaConsumerProperties.KEY_COMMIT_TIMEOUT_MS, 5 * 60 * 1000L),
                config.getInteger(KafkaConsumerProperties.KEY_MAX_RETRIES, Integer.MAX_VALUE),
                config.getInteger(KafkaConsumerProperties.KEY_INITIAL_RETRY_DELAY_SECONDS, 1),
                config.getInteger(KafkaConsumerProperties.KEY_MAX_RETRY_DELAY_SECONDS, 10),
                config.getLong(KafkaConsumerProperties.EVENT_BUS_SEND_TIMEOUT, DeliveryOptions.DEFAULT_TIMEOUT),
                config.getDouble(KafkaConsumerProperties.MESSAGES_PER_SECOND, -1D),
                config.getBoolean(KafkaConsumerProperties.COMMIT_ON_PARTITION_CHANGE, true),
                config.getBoolean(KafkaConsumerProperties.STRICT_ORDERING, false),
                config.getInteger(KafkaConsumerProperties.MAX_POLL_RECORDS, 500)
        );

        consumer = KafkaConsumerManager.create(vertx, configuration, this::handler);
        consumer.start();
    }

    private String getMandatoryStringConfig(final JsonObject jsonObject, final String key) {
        final String value = jsonObject.getString(key);
        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException("No configuration for key " + key + " found");
        }
        return value;
    }

    private void handler(final String message, final Future<Void> futureResult) {
        final DeliveryOptions options = new DeliveryOptions();
        options.setSendTimeout(configuration.getEventBusSendTimeout());

        vertx.eventBus().send(vertxAddress, message, options, (result) -> {
            if (result.succeeded()) {
                futureResult.complete();
            } else {
                futureResult.fail(result.cause());
            }
        });
    }

    @Override
    public void stop() throws Exception {
        if (consumer != null) {
            consumer.stop();
        }
        super.stop();
    }
}
