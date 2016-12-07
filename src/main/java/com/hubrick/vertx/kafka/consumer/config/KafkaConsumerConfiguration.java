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
package com.hubrick.vertx.kafka.consumer.config;

/**
 * Configuration Options for the Kafka Consumer.
 *
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public class KafkaConsumerConfiguration {

    private final String groupId;
    private final String kafkaTopic;
    private final String zookeeper;
    private final String offsetReset;
    private final int zookeeperTimeout;
    private final int maxUnacknowledged;
    private final long maxUncommitedOffsets;
    private final long ackTimeoutSeconds;
    private final long commitTimeoutMs;
    private final int maxRetries;
    private final int initialRetryDelaySeconds;
    private final int maxRetryDelaySeconds;
    private final long eventBusSendTimeout;
    private final long messageMaxBytes;

    private KafkaConsumerConfiguration(final String groupId,
                                       final String kafkaTopic,
                                       final String zookeeper,
                                       final String offsetReset,
                                       final int zookeeperTimeout,
                                       final int maxUnacknowledged,
                                       final long maxUncommittedOffset,
                                       final long ackTimeoutSeconds,
                                       final long commitTimeoutMs,
                                       final int maxRetries,
                                       final int initialRetryDelaySeconds,
                                       final int maxRetryDelaySeconds,
                                       final long eventBusSendTimeout,
                                       final long messageMaxBytes) {
        this.groupId = groupId;
        this.kafkaTopic = kafkaTopic;
        this.zookeeper = zookeeper;
        this.offsetReset = offsetReset;
        this.zookeeperTimeout = zookeeperTimeout;
        this.maxUnacknowledged = maxUnacknowledged;
        this.maxUncommitedOffsets = maxUncommittedOffset;
        this.ackTimeoutSeconds = ackTimeoutSeconds;
        this.commitTimeoutMs = commitTimeoutMs;
        this.maxRetries = maxRetries;
        this.initialRetryDelaySeconds = initialRetryDelaySeconds;
        this.maxRetryDelaySeconds = maxRetryDelaySeconds;
        this.eventBusSendTimeout = eventBusSendTimeout;
        this.messageMaxBytes = messageMaxBytes;
    }

    public static KafkaConsumerConfiguration create(final String groupId,
                                                    final String kafkaTopic,
                                                    final String zookeeper,
                                                    final String offsetReset,
                                                    final int zookeeperTimeout,
                                                    final int maxUnacknowledged,
                                                    final long maxUncommittedOffsets,
                                                    final long ackTimeoutSeconds,
                                                    final long commitTimeoutMs,
                                                    final int maxRetries,
                                                    final int initialRetryDelaySeconds,
                                                    final int maxRetryDelaySeconds,
                                                    final long eventBusSendTimeout,
                                                    final long messageMaxBytes) {
        return new KafkaConsumerConfiguration(groupId,
                kafkaTopic,
                zookeeper,
                offsetReset,
                zookeeperTimeout,
                maxUnacknowledged,
                maxUncommittedOffsets,
                ackTimeoutSeconds,
                commitTimeoutMs,
                maxRetries,
                initialRetryDelaySeconds,
                maxRetryDelaySeconds,
                eventBusSendTimeout,
                messageMaxBytes);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public int getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public int getMaxUnacknowledged() {
        return maxUnacknowledged;
    }

    public long getMaxUncommitedOffsets() {
        return maxUncommitedOffsets;
    }

    public long getAckTimeoutSeconds() {
        return ackTimeoutSeconds;
    }

    public long getCommitTimeoutMs() {
        return commitTimeoutMs;
    }

    public int getMaxRetryDelaySeconds() {
        return maxRetryDelaySeconds;
    }

    public int getInitialRetryDelaySeconds() {
        return initialRetryDelaySeconds;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getEventBusSendTimeout() {
        return eventBusSendTimeout;
    }

    public long getMessageMaxBytes() {
        return messageMaxBytes;
    }
}
