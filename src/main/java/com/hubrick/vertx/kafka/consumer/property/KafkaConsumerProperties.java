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
package com.hubrick.vertx.kafka.consumer.property;

/**
 * Configuration Options for the Kafka Consumer.
 *
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public final class KafkaConsumerProperties {

    private KafkaConsumerProperties() {}

    public static final String KEY_VERTX_ADDRESS = "address";

    public static final String KEY_CLIENT_ID = "clientId";
    public static final String KEY_GROUP_ID = "groupId";
    public static final String KEY_KAFKA_TOPIC = "kafkaTopic";
    public static final String KEY_BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String KEY_OFFSET_RESET = "offsetReset";
    public static final String KEY_MAX_UNACKNOWLEDGED = "maxUnacknowledged";
    public static final String KEY_MAX_UNCOMMITTED_OFFSETS = "maxUncommitted";
    public static final String KEY_ACK_TIMEOUT_SECONDS = "ackTimeoutSeconds";
    public static final String KEY_COMMIT_TIMEOUT_MS = "commitTimeoutMs";
    public static final String KEY_MAX_RETRIES = "maxRetries";
    public static final String KEY_INITIAL_RETRY_DELAY_SECONDS = "initialRetryDelaySeconds";
    public static final String KEY_MAX_RETRY_DELAY_SECONDS = "maxRetryDelaySeconds";
    public static final String EVENT_BUS_SEND_TIMEOUT = "eventBusSendTimeout";
}
