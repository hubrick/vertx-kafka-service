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

import com.google.common.math.IntMath;
import com.google.common.util.concurrent.RateLimiter;
import com.hubrick.vertx.kafka.consumer.config.KafkaConsumerConfiguration;
import com.hubrick.vertx.kafka.consumer.util.PrometheusMetrics;
import com.hubrick.vertx.kafka.consumer.util.ThreadFactoryUtil;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marcus Thiesen
 * @since 1.0.0
 */
class KafkaConsumerManager {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final Vertx vertx;
    private final KafkaConsumer<String,String> consumer;
    private final KafkaConsumerConfiguration configuration;
    private final KafkaConsumerHandler handler;

    private final ExecutorService messageProcessorExececutor = Executors.newSingleThreadExecutor(ThreadFactoryUtil.createThreadFactory("kafka-consumer-thread-%d", LOG));

    private final Phaser phaser = new Phaser() {
        @Override
        protected boolean onAdvance(int phase, int registeredParties) {
            LOG.debug("{}: Advance: Phase {}, registeredParties {}", configuration.getKafkaTopic(), phase, registeredParties);
            return false;
        }
    };
    private final Set<Long> unacknowledgedOffsets = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
    private final AtomicLong lastCommittedOffset = new AtomicLong();
    private final AtomicLong lastReadOffset = new AtomicLong();
    private final AtomicLong currentPartition = new AtomicLong(-1);
    private final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());
    private final Optional<RateLimiter> rateLimiter;
    private final PrometheusMetrics prometheusMetrics;
    private final AtomicBoolean waiting = new AtomicBoolean(false);
    private final AtomicInteger lastPhase = new AtomicInteger(-1);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    public KafkaConsumerManager(final Vertx vertx,
                                final KafkaConsumer<String, String> consumer,
                                final KafkaConsumerConfiguration configuration,
                                final KafkaConsumerHandler handler,
                                final PrometheusMetrics prometheusMetrics) {
        this.vertx = vertx;
        this.consumer = consumer;
        this.configuration = configuration;
        this.handler = handler;
        this.rateLimiter = configuration.getMessagesPerSecond() > 0.0D ?
                    Optional.of(RateLimiter.create(configuration.getMessagesPerSecond())) : Optional.empty();
        this.prometheusMetrics = prometheusMetrics;

        LOG.info("Created Kafka Consumer Manager with the following configuration: {}", this.configuration);
    }

    public static KafkaConsumerManager create(final Vertx vertx,
                                              final KafkaConsumerConfiguration configuration,
                                              final PrometheusMetrics prometheusMetrics,
                                              final KafkaConsumerHandler handler) {
        final Map<String, Object> properties = createProperties(configuration);
        final KafkaConsumer consumer = new KafkaConsumer(properties, new StringDeserializer(), new StringDeserializer());
        return new KafkaConsumerManager(vertx, consumer, configuration, handler, prometheusMetrics);
    }

    protected static Map<String, Object> createProperties(KafkaConsumerConfiguration configuration) {
        final Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, configuration.getClientId());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, configuration.getGroupId());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configuration.getOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(configuration.getMaxPollRecords()));
        properties.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, configuration.getMetricConsumerClasses());
        properties.put("metric.dropwizard.registry", String.valueOf(configuration.getMetricDropwizardRegistryName()));

        return properties;
    }

    public void stop() {
        shutdownRequested.set(true);
        messageProcessorExececutor.shutdownNow();
    }

    private void executeStopConsumer() {
        consumer.unsubscribe();
        consumer.close();
    }

    /**
     * Starts the message consuming loop.
     *
     * @param startedFuture to try to complete once the message consuming loop started
     * @return Future that completes if the message consuming loop exited
     */
    public java.util.concurrent.Future<?> start(final Future<Void> startedFuture) {
        final String kafkaTopic = configuration.getKafkaTopic();
        consumer.subscribe(Collections.singletonList(kafkaTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                LOG.info("{}: Partitions revoked", configuration.getKafkaTopic());
                if (lastPhase.get() == -1) {
                    LOG.info("{}: Nothing consumed yet, nothing to commit", configuration.getKafkaTopic());
                    return;
                }
                if (!waitForAcks(lastPhase.get())) {
                    return;
                }
                commitOffsetsIfAllAcknowledged(lastReadOffset.get());
                LOG.info("{}: Commited on partitions revoked", configuration.getKafkaTopic());
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}

        });

        return messageProcessorExececutor.submit(() -> {
            startedFuture.tryComplete();
            try {
                read();
            } catch (final Exception e) {
                executeStopConsumer();
                throw new RuntimeException(e);
            }
        });
    }

    private void read() {
        while (!consumer.subscription().isEmpty()) {
            final ConsumerRecords<String, String> records = consumer.poll(60000);
            if (shutdownRequested.get()) {
                executeStopConsumer();
            }
            final Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                if (shutdownRequested.get()) {
                    executeStopConsumer();
                    return;
                }
                rateLimiter.ifPresent(limiter -> limiter.acquire());
                final PrometheusMetrics.InProgressMessage inProgressMessage = prometheusMetrics.messageStarted();

                final int phase = phaser.register();
                lastPhase.set(phase);

                final ConsumerRecord<String, String> msg = iterator.next();
                final long offset = msg.offset();
                final long partition = msg.partition();
                unacknowledgedOffsets.add(offset);
                lastReadOffset.set(offset);
                lastCommittedOffset.compareAndSet(0, offset);
                currentPartition.compareAndSet(-1, partition);

                handle(msg.value(), partition, offset, configuration.getMaxRetries(), configuration.getInitialRetryDelaySeconds(), inProgressMessage);

                if (unacknowledgedOffsets.size() >= configuration.getMaxUnacknowledged()
                        || partititionChanged(partition)
                        || tooManyUncommittedOffsets(offset)
                        || commitTimeoutReached()) {
                    LOG.info("{}: Got {} unacknowledged messages, waiting for ACKs in order to commit",
                            configuration.getKafkaTopic(),
                            unacknowledgedOffsets.size());
                    if (!waitForAcks(phase)) {
                        return;
                    }
                    commitOffsetsIfAllAcknowledged(offset);
                    LOG.info("{}: Continuing message processing on partition {}", configuration.getKafkaTopic(), currentPartition.get());
                }
            }
        }
        LOG.info("{}: ConsumerManager:read exited loop, consuming of messages has ended.", configuration.getKafkaTopic());
    }

    private void handle(final String msg,
                        final Long partition,
                        final Long offset,
                        final int tries,
                        final int delaySeconds,
                        final PrometheusMetrics.InProgressMessage inProgressMessage) {
        final Future<Void> futureResult = Future.future();
        final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        final int nextDelaySeconds = computeNextDelay(delaySeconds);
        final int nextTry = tries - 1;

        futureResult.setHandler(result -> {
            if (result.succeeded()) {
                completableFuture.complete(null);
                phaser.arriveAndDeregister();
                unacknowledgedOffsets.remove(offset);

                if (waiting.get()) {
                    LOG.info("{}: Acknowledged event from partition {} at offset {}, still not acknowledged {}:\n{}",
                            configuration.getKafkaTopic(),
                            partition,
                            offset,
                            unacknowledgedOffsets.size(),
                            msg
                    );
                }
                inProgressMessage.finished();

            } else {
                completableFuture.completeExceptionally(result.cause());
                if (tries > 0) {
                    if (!configuration.isStrictOrderingEnabled()) {
                        LOG.error("{}: Exception occurred during kafka message processing at offset {} on partition {}, will retry in {} seconds ({} remaining tries): {}",
                                configuration.getKafkaTopic(),
                                offset,
                                partition,
                                delaySeconds,
                                tries,
                                msg,
                                result.cause());

                        vertx.setTimer(delaySeconds * 1000, event -> handle(msg, partition, offset, nextTry, nextDelaySeconds, inProgressMessage));
                    }

                } else {
                    LOG.error("{}: Exception occurred during kafka message processing at offset {} on partition {}. Max number of retries reached. Skipping message: {}",
                            configuration.getKafkaTopic(),
                            offset,
                            partition,
                            msg,
                            result.cause());
                    unacknowledgedOffsets.remove(offset);
                    phaser.arriveAndDeregister();
                    inProgressMessage.finished();
                }
            }
        });

        handler.handle(msg, futureResult);

        if (configuration.isStrictOrderingEnabled()) {
            try {
                completableFuture.get(configuration.getAckTimeoutSeconds(), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("{}: Interrupted while waiting for strict ACK", configuration.getKafkaTopic(), e);
            } catch (ExecutionException e) {
                if (tries > 0) {
                    LOG.error("{}: Exception occurred during kafka message processing in strict mode at offset {} on partition {}, will retry in {} seconds ({} remaining tries): {}",
                            configuration.getKafkaTopic(),
                            offset,
                            partition,
                            delaySeconds,
                            tries,
                            msg,
                            e);
                    try {
                        Thread.sleep(delaySeconds * 1000);
                    } catch (InterruptedException e1) {
                        LOG.error("{}: Interrupted while waiting for retry", configuration.getKafkaTopic(), e1);
                    }
                    handle(msg, partition, offset, nextTry, nextDelaySeconds, inProgressMessage);
                }
            } catch (TimeoutException e) {
                LOG.error("{}: Waited for {} strict ACKs for longer than {} seconds, not making any progress", new Object[]{
                        configuration.getKafkaTopic(),
                        Integer.valueOf(unacknowledgedOffsets.size()),
                        Long.valueOf(configuration.getAckTimeoutSeconds()),
                    });
            }
        }
    }

    private boolean commitTimeoutReached() {
        return System.currentTimeMillis() - lastCommitTime.get() >= configuration.getCommitTimeoutMs();
    }

    private boolean partititionChanged(long partition) {
        if (currentPartition.get() != partition) {
            LOG.info("{}: Partition changed from {} while having {} unacknowledged messages to partition {}"
            + (configuration.isCommitOnPartitionChange() ? " initiating commit" : " continuing processing"),
                    configuration.getKafkaTopic(),
                    currentPartition.get(),
                    unacknowledgedOffsets.size(),
                    partition);
            currentPartition.set(partition);
            return configuration.isCommitOnPartitionChange();
        }
        return false;
    }

    private int computeNextDelay(int delaySeconds) {
        try {
            return Math.min(IntMath.checkedMultiply(delaySeconds, 2), configuration.getMaxRetryDelaySeconds());
        } catch (ArithmeticException e) {
            return configuration.getMaxRetryDelaySeconds();
        }
    }

    private boolean waitForAcks(int phase) {
        if (shutdownRequested.get()) {
            executeStopConsumer();
            return false;
        }
        try {
            waiting.set(true);
            phaser.awaitAdvanceInterruptibly(phase, configuration.getAckTimeoutSeconds(), TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            LOG.error("{}: Interrupted while waiting for ACKs", configuration.getKafkaTopic(), e);
            return false;
        } catch (TimeoutException e) {
            LOG.error("{}: Waited for {} ACKs for longer than {} seconds, not making any progress ({}/{})", new Object[]{
                    configuration.getKafkaTopic(),
                    Integer.valueOf(unacknowledgedOffsets.size()),
                    Long.valueOf(configuration.getAckTimeoutSeconds()),
                    Integer.valueOf(phase),
                    Integer.valueOf(phaser.getPhase())});
            return waitForAcks(phase);
        } finally {
            waiting.set(false);
        }
    }

    private boolean tooManyUncommittedOffsets(final long offset) {
        return lastCommittedOffset.get() + configuration.getMaxUncommitedOffsets() <= offset;
    }

    private void commitOffsetsIfAllAcknowledged(final long currentOffset) {
        if (unacknowledgedOffsets.isEmpty()) {
            LOG.info("{}: Committing partition {} at offset {} (and all former partition offsets)", configuration.getKafkaTopic(), currentPartition.get(), currentOffset);
            consumer.commitSync();
            lastCommittedOffset.set(currentOffset);
            lastCommitTime.set(System.currentTimeMillis());
            LOG.info("{}: Committing partition {} at offset {} (and all former partition offsets) was successful", configuration.getKafkaTopic(), currentPartition.get(), currentOffset);
        } else {
            LOG.warn("{}: Can not commit because {} ACKs missing", configuration.getKafkaTopic(), unacknowledgedOffsets.size());
        }
    }
}
