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
package com.hubrick.vertx.kafka.producer.property;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public final class KafkaProducerProperties {

    /* Non-instantiable class */
    private KafkaProducerProperties() {}

    public static final String ADDRESS = "address";
    public static final String BOOTSTRAP_SERVERS = "bootstrapServers";
    public static final String ACKS = "acks";
    public static final String DEFAULT_TOPIC = "defaultTopic";
    public static final String TYPE = "type";
    public static final String MAX_RETRIES = "maxRetries";
    public static final String RETRY_BACKOFF_MS = "retryBackoffMs";
    public static final String BUFFERING_MAX_MS = "bufferingMaxMs";
    public static final String BUFFERING_MAX_MESSAGES = "bufferingMaxMessages";
    public static final String ENQUEUE_TIMEOUT = "enqueueTimeout";
    public static final String BATCH_MESSAGE_NUM = "batchMessageNum";
    public static final String RETRIES = "retries";
    public static final String REQUEST_TIMEOUT_MS = "requestTimeoutMs";
    public static final String MAX_BLOCK_MS = "maxBlockMs";
    public static final String STATSD = "statsD";

    public static final String BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092";
    public static final String ACKS_DEFAULT = "1";

    public static final int RETRIES_DEFAULT = 0;
    public static final int REQUEST_TIMEOUT_MS_DEFAULT = 30000;
    public static final int MAX_BLOCK_MS_DEFAULT = 60000;

}
