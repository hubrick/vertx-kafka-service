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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.math.IntMath;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hubrick.vertx.kafka.consumer.config.KafkaConsumerConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marcus Thiesen
 * @since 1.0.0
 */
class KafkaConsumer {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private final static ThreadFactory FACTORY = new ThreadFactoryBuilder()
                                                    .setNameFormat("kafka-consumer-thread-%d")
                                                    .setUncaughtExceptionHandler((thread, throwable) -> LOG.error("Uncaught exception in thread {}", thread.getName(), throwable))
                                                    .setDaemon(true)
                                                    .build();

    private final Vertx vertx;
    private final ConsumerConnector connector;
    private final KafkaConsumerConfiguration configuration;
    private final KafkaConsumerHandler handler;
    private final Set<Long> unacknowledgedOffsets = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
    private final ExecutorService messageProcessorExececutor = Executors.newSingleThreadExecutor(FACTORY);
    private final Phaser phaser = new Phaser() {
        @Override
        protected boolean onAdvance(int phase, int registeredParties) {
            LOG.debug("Advance: Phase {}, registeredParties {}", phase, registeredParties);
            return false;
        }
    };
    private final AtomicLong lastCommittedOffset = new AtomicLong();
    private final AtomicLong currentPartition = new AtomicLong(-1);
    private final AtomicLong lastCommitTime = new AtomicLong(System.currentTimeMillis());

    public KafkaConsumer(Vertx vertx, ConsumerConnector connector, KafkaConsumerConfiguration configuration, KafkaConsumerHandler handler) {
        this.vertx = vertx;
        this.connector = connector;
        this.configuration = configuration;
        this.handler = handler;
    }

    public static KafkaConsumer create(final Vertx vertx, final KafkaConsumerConfiguration configuration, final KafkaConsumerHandler handler) {
        final Properties properties = createProperties(configuration);
        final ConsumerConfig config = new ConsumerConfig(properties);
        final ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
        return new KafkaConsumer(vertx, connector, configuration, handler);
    }

    protected static Properties createProperties(KafkaConsumerConfiguration configuration) {
        final Properties properties = new Properties();

        properties.setProperty("zookeeper.connect", configuration.getZookeeper());
        properties.setProperty("group.id", configuration.getGroupId());
        properties.setProperty("zookeeper.connection.timeout.ms", Integer.toString(configuration.getZookeeperTimeout()));
        properties.setProperty("auto.commit.enable", Boolean.FALSE.toString());
        properties.setProperty("auto.offset.reset", configuration.getOffsetReset());
        properties.setProperty("queued.max.message.chunks", "1000");

        return properties;
    }

    public void stop() {
        messageProcessorExececutor.shutdownNow();
        connector.shutdown();
    }

    public void start() {
        final String kafkaTopic = configuration.getKafkaTopic();
        final Map<String, List<KafkaStream<String, String>>> messageStreams = connector.createMessageStreams(ImmutableMap.of(kafkaTopic, 1),
                new StringDecoder(null),
                new StringDecoder(null));
        final List<KafkaStream<String, String>> topicStreams = messageStreams.get(kafkaTopic);
        final KafkaStream<String, String> topicStream = Iterables.getOnlyElement(topicStreams);

        messageProcessorExececutor.submit(() -> read(topicStream));
    }

    private void read(final KafkaStream<String, String> stream) {
        while (stream.iterator().hasNext()) {
            final int phase = phaser.register();

            final MessageAndMetadata<String, String> msg = stream.iterator().next();
            final long offset = msg.offset();
            final long partition = msg.partition();
            unacknowledgedOffsets.add(offset);
            lastCommittedOffset.compareAndSet(0, offset);
            currentPartition.compareAndSet(-1, partition);

            handle(msg.message(), offset, configuration.getMaxRetries(), configuration.getInitialRetryDelaySeconds());

            if (unacknowledgedOffsets.size() >= configuration.getMaxUnacknowledged()
                    || partititionChanged(partition)
                    || tooManyUncommittedOffsets(offset)
                    || commitTimeoutReached()) {
                LOG.info("Got {} unacknowledged messages, waiting for ACKs in order to commit", unacknowledgedOffsets.size());
                if (!waitForAcks(phase)) {
                    return;
                }
                commitOffsetsIfAllAcknowledged(offset);
                LOG.info("Continuing message processing");
            }
        }
    }

    private void handle(String msg, Long offset, int tries, int delaySeconds) {
        final Future<Void> futureResult = Future.future();
        futureResult.setHandler(result -> {
            if(result.succeeded()) {
                unacknowledgedOffsets.remove(offset);
                phaser.arriveAndDeregister();
            } else {
                final int nextDelaySeconds = computeNextDelay(delaySeconds);
                if (tries > 0) {
                    LOG.error("Exception occurred during kafka message processing, will retry in {} seconds: {}", delaySeconds, msg, result.cause());
                    final int nextTry = tries - 1;
                    vertx.setTimer(delaySeconds * 1000, event -> handle(msg, offset, nextTry, nextDelaySeconds));
                } else {
                    LOG.error("Exception occurred during kafka message processing. Max number of retries reached. Skipping message: {}", msg, result.cause());
                    unacknowledgedOffsets.remove(offset);
                    phaser.arriveAndDeregister();
                }
            }
        });
        handler.handle(msg, futureResult);
    }

    private boolean commitTimeoutReached() {
        return System.currentTimeMillis() - lastCommitTime.get() >= configuration.getCommitTimeoutMs();
    }

    private boolean partititionChanged(long partition) {
        if (currentPartition.get() != partition) {
            LOG.info("Partition changed from {} to {}", currentPartition.get(), partition);
            currentPartition.set(partition);
            return true;
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
        try {
            phaser.awaitAdvanceInterruptibly(phase, configuration.getAckTimeoutSeconds(), TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for ACKs", e);
            return false;
        } catch (TimeoutException e) {
            LOG.error("Waited for {} ACKs for longer than {} seconds, not making any progress ({}/{})", new Object[]{
                    Integer.valueOf(unacknowledgedOffsets.size()), Long.valueOf(configuration.getAckTimeoutSeconds()),
                    Integer.valueOf(phase), Integer.valueOf(phaser.getPhase())});
            return waitForAcks(phase);
        }
    }

    private boolean tooManyUncommittedOffsets(final long offset) {
        return lastCommittedOffset.get() + configuration.getMaxUncommitedOffsets() <= offset;
    }

    private void commitOffsetsIfAllAcknowledged(final long currentOffset) {
        if (unacknowledgedOffsets.isEmpty()) {
            LOG.info("Committing at offset {}", currentOffset);
            connector.commitOffsets();
            lastCommittedOffset.set(currentOffset);
            lastCommitTime.set(System.currentTimeMillis());
        } else {
            LOG.warn("Can not commit because {} ACKs missing", unacknowledgedOffsets.size());
        }
    }
}
