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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.hubrick.vertx.kafka.consumer.config.KafkaConsumerConfiguration;
import com.hubrick.vertx.kafka.consumer.property.KafkaConsumerProperties;
import com.hubrick.vertx.kafka.consumer.util.PrometheusMetrics;
import com.hubrick.vertx.kafka.consumer.util.ThreadFactoryUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vert.x Module to read from a Kafka Topic.
 *
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public class KafkaConsumerVerticle extends AbstractVerticle {

    static final Double NON_STRICT_ORDERING_MESSAGES_PER_SECOND_DEFAULT = 20D;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerVerticle.class);
    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong();
    private static final Splitter COMMA_LIST_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final ThreadFactory CONSUMER_WATCHER_THREAD = ThreadFactoryUtil.createThreadFactory("kafka-consumer-watcher-thread-%d", LOG);
    private ExecutorService watcherExecutor = Executors.newSingleThreadExecutor(CONSUMER_WATCHER_THREAD);

    private volatile KafkaConsumerManager consumer;


    @Override
    public void start(final Future<Void> startedFuture) throws Exception {
        final JsonObject config = vertx.getOrCreateContext().config();
        final long instanceId = INSTANCE_COUNTER.getAndIncrement();
        final String vertxAddress = getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_VERTX_ADDRESS);

        final KafkaConsumerConfiguration configuration = createKafkaConsumerConfiguration(config, instanceId);

        final PrometheusMetrics prometheusMetrics = PrometheusMetrics.create(configuration.getKafkaTopic(), configuration.getGroupId(), instanceId);

        watcherExecutor.execute(() -> watchStartConsumerManager(configuration, vertxAddress, startedFuture, prometheusMetrics));
    }

    KafkaConsumerConfiguration createKafkaConsumerConfiguration(final JsonObject config, final long instanceId) {
        final String clientIdPrefix = getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_CLIENT_ID);

        final String topic = getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_KAFKA_TOPIC);
        final String consumerGroup = getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_GROUP_ID);

        final Boolean strictOrderingEnabled = config.getBoolean(KafkaConsumerProperties.KEY_STRICT_ORDERING, true);

        final Double messagesPerSecond = config.getDouble(KafkaConsumerProperties.KEY_MESSAGES_PER_SECOND, -1D);
        final Integer maxPollRecords = config.getInteger(KafkaConsumerProperties.KEY_MAX_POLL_RECORDS, 500);
        final Long maxPollIntervalMs = config.getLong(KafkaConsumerProperties.KEY_MAX_POLL_INTERVAL_MS, 300000L);
        final double maxPollIntervalSeconds = maxPollIntervalMs / 1000D;

        final Double effectiveMessagesPerSecond;
        if ((!strictOrderingEnabled) && messagesPerSecond < 0D) {
            effectiveMessagesPerSecond = NON_STRICT_ORDERING_MESSAGES_PER_SECOND_DEFAULT;
            LOG.warn("Strict ordering is disabled but no message limit given, limiting to {}", effectiveMessagesPerSecond);
        } else {
            effectiveMessagesPerSecond = messagesPerSecond;
        }

        final long effectiveMaxPollIntervalMs;
        if (effectiveMessagesPerSecond > 0D && effectiveMessagesPerSecond < (maxPollRecords / maxPollIntervalSeconds)) {
            effectiveMaxPollIntervalMs = (long) Math.ceil(maxPollRecords / effectiveMessagesPerSecond * 1000) * 2;
            LOG.warn("The configuration limits to handling {} messages per seconds, but number of polled records is {} that should be handled in {}ms." +
                            " This will cause the consumer to be marked as dead if there are more messages on the topic than are handled per second." +
                            " Setting the maxPollInterval to {}ms",
                    effectiveMessagesPerSecond, maxPollRecords, maxPollIntervalMs, effectiveMaxPollIntervalMs);
        } else {
            effectiveMaxPollIntervalMs = maxPollIntervalMs;
        }

        return KafkaConsumerConfiguration.create(
                consumerGroup,
                clientIdPrefix + "-" + instanceId,
                topic,
                getMandatoryStringConfig(config, KafkaConsumerProperties.KEY_BOOTSTRAP_SERVERS),
                config.getString(KafkaConsumerProperties.KEY_OFFSET_RESET, "latest"),
                config.getInteger(KafkaConsumerProperties.KEY_MAX_UNACKNOWLEDGED, 100),
                config.getLong(KafkaConsumerProperties.KEY_MAX_UNCOMMITTED_OFFSETS, 1000L),
                config.getLong(KafkaConsumerProperties.KEY_ACK_TIMEOUT_SECONDS, 240L),
                config.getLong(KafkaConsumerProperties.KEY_COMMIT_TIMEOUT_MS, 5 * 60 * 1000L),
                config.getInteger(KafkaConsumerProperties.KEY_MAX_RETRIES, Integer.MAX_VALUE),
                config.getInteger(KafkaConsumerProperties.KEY_INITIAL_RETRY_DELAY_SECONDS, 1),
                config.getInteger(KafkaConsumerProperties.KEY_MAX_RETRY_DELAY_SECONDS, 10),
                config.getLong(KafkaConsumerProperties.KEY_EVENT_BUS_SEND_TIMEOUT, DeliveryOptions.DEFAULT_TIMEOUT),
                effectiveMessagesPerSecond,
                config.getBoolean(KafkaConsumerProperties.KEY_COMMIT_ON_PARTITION_CHANGE, true),
                strictOrderingEnabled,
                maxPollRecords,
                effectiveMaxPollIntervalMs,
                COMMA_LIST_SPLITTER.splitToList(config.getString(KafkaConsumerProperties.KEY_METRIC_CONSUMER_CLASSES, "")),
                config.getString(KafkaConsumerProperties.KEY_METRIC_DROPWIZARD_REGISTRY_NAME, "")
        );
    }


    private void watchStartConsumerManager(final KafkaConsumerConfiguration configuration,
                                           final String vertxAddress,
                                           final Future<Void> startedFuture,
                                           final PrometheusMetrics prometheusMetrics) {
        try {
            final java.util.concurrent.Future<?> future = startConsumerManager(configuration, vertxAddress, startedFuture, prometheusMetrics);
            future.get();
            LOG.info("{}: Consumer manager run loop has returned, restarting", configuration.getKafkaTopic());
            stopConsumerManager();
            watcherExecutor.execute(() -> watchStartConsumerManager(configuration, vertxAddress, startedFuture, prometheusMetrics));
        } catch (InterruptedException e) {
            LOG.info("{}: ConsumerManager got interrupted, returning", configuration.getKafkaTopic());
            stopConsumerManager();
            watcherExecutor.shutdownNow();
        } catch (ExecutionException e) {
            LOG.warn("{}: ExecutionException in consumer manager, restarting", configuration.getKafkaTopic(), e);
            stopConsumerManager();
            watcherExecutor.execute(() -> watchStartConsumerManager(configuration, vertxAddress, startedFuture, prometheusMetrics));
        } catch (RetriableException e) {
            LOG.warn("{}: RetriableException in consumer manager, restarting", configuration.getKafkaTopic(), e);
            stopConsumerManager();
            watcherExecutor.execute(() -> watchStartConsumerManager(configuration, vertxAddress, startedFuture, prometheusMetrics));
        } catch (KafkaException e) {
            LOG.error("{}: KafkaException in consumer manager, returning", configuration.getKafkaTopic(), e);
            stopConsumerManager();
            watcherExecutor.shutdownNow();
            startedFuture.tryFail(e);
        }
    }

    private java.util.concurrent.Future<?> startConsumerManager(final KafkaConsumerConfiguration configuration,
                                                                final String vertxAddress,
                                                                final Future<Void> startedFuture,
                                                                final PrometheusMetrics prometheusMetrics) {
        consumer = KafkaConsumerManager.create(vertx, configuration, prometheusMetrics, makeHandler(configuration, vertxAddress));
        return consumer.start(startedFuture);
    }

    private String getMandatoryStringConfig(final JsonObject jsonObject, final String key) {
        final String value = jsonObject.getString(key);
        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException("No configuration for key " + key + " found");
        }
        return value;
    }

    private KafkaConsumerHandler makeHandler(final KafkaConsumerConfiguration configuration, final String vertxAddress) {
        return (message, futureResult) -> {
            final DeliveryOptions options = new DeliveryOptions();
            options.setSendTimeout(configuration.getEventBusSendTimeout());

            vertx.eventBus().send(vertxAddress, message, options, (result) -> {
                if (result.succeeded()) {
                    futureResult.complete();
                } else {
                    futureResult.fail(result.cause());
                }
            });
        };
    }

    @Override
    public void stop() throws Exception {
        stopConsumerManager();
        super.stop();
    }

    private void stopConsumerManager() {
        if (consumer != null) {
            consumer.stop();
        }
    }
}
