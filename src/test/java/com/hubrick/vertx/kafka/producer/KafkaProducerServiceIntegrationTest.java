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

import com.google.common.collect.Lists;
import com.googlecode.junittoolbox.PollingWait;
import com.googlecode.junittoolbox.RunnableAssert;
import com.hubrick.vertx.kafka.producer.model.StringKafkaMessage;
import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import com.netflix.curator.test.TestingServer;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
@RunWith(VertxUnitRunner.class)
public class KafkaProducerServiceIntegrationTest extends AbstractProducerTest {

    private static final String ADDRESS = "default-address";
    private static final String TOPIC = "some-topic";

    private TestingServer zookeeper;
    private KafkaServer kafkaServer;
    private ConsumerConnector consumer;
    private List<String> messagesReceived = new ArrayList<String>();
    private PollingWait wait = new PollingWait().timeoutAfter(10, TimeUnit.MINUTES).pollEvery(100, TimeUnit.MILLISECONDS);

    @Before
    public void setUp() {
        startZookeeper();
        startKafkaServer();
        createTopic(KafkaProducerProperties.DEFAULT_TOPIC);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        consumeMessages();
    }

    @After
    public void tearDown() {
        consumer.shutdown();
        if (null != kafkaServer) {
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
        }
        if (null != zookeeper) {
            try {
                zookeeper.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void startZookeeper() {
        try {
            zookeeper = new TestingServer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test(TestContext testContext) throws Exception {
        JsonObject config = new JsonObject();
        config.put(KafkaProducerProperties.ADDRESS, ADDRESS);
        config.put(KafkaProducerProperties.BOOTSTRAP_SERVERS, KafkaProducerProperties.BOOTSTRAP_SERVERS_DEFAULT);
        config.put(KafkaProducerProperties.DEFAULT_TOPIC, TOPIC);
        config.put(KafkaProducerProperties.ACKS, KafkaProducerProperties.ACKS_DEFAULT);

        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        deploy(testContext, deploymentOptions);

        final Async async = testContext.async();
        try {
            shouldReceiveMessage(testContext, async);
        } catch (Exception e) {
            testContext.fail(e);
        }
    }

    public void shouldReceiveMessage(TestContext testContext, Async async) throws Exception {
        final StringKafkaMessage stringKafkaMessage = new StringKafkaMessage("foobar", "bar");
        final KafkaProducerService kafkaProducerService = KafkaProducerService.createProxy(vertx, ADDRESS);
        kafkaProducerService.sendString(stringKafkaMessage, (Handler<AsyncResult<Void>>) message -> {
            if (message.failed()) {
                testContext.fail();
            }
        });

        wait.until(new RunnableAssert("shouldReceiveMessage") {
            @Override
            public void run() throws Exception {
                assertThat(messagesReceived.contains("foobar"), is(true));
            }
        });
        async.complete();
    }

    private KafkaServer startKafkaServer() {
        Properties props = TestUtils.createBrokerConfig(0, zookeeper.getConnectString(), true, true, 9092, Option.<SecurityProtocol>empty(), Option.<File>empty(), Option.<Properties>empty(), true, false, 0, false, 0, false, 0, Option.<String>empty(), 0);
        props.put(KafkaConfig.ZkConnectProp(), zookeeper.getConnectString());
        kafkaServer = TestUtils.createServer(new KafkaConfig(props), new kafka.utils.MockTime());
        return kafkaServer;
    }

    private void createTopic(String topic) {
        final ZkClient zkClient = new ZkClient(zookeeper.getConnectString(), 30000, 30000, ZKStringSerializer$.MODULE$);
        final ZkConnection connection = new ZkConnection(zookeeper.getConnectString());
        final ZkUtils zkUtils = new ZkUtils(zkClient, connection, false);
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        TestUtils.waitUntilMetadataIsPropagated(JavaConversions.asScalaBuffer(Lists.newArrayList(kafkaServer)), topic, 0, 10000);
        zkClient.close();
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", zookeeper.getConnectString());
        props.put("group.id", "group.test");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("auto.offset.reset", "smallest");
        return new ConsumerConfig(props);
    }

    private void consumeMessages() {
        final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, 1);
        final StringDecoder decoder =
                new StringDecoder(new VerifiableProperties());
        final Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, decoder, decoder);
        final KafkaStream<String, String> stream =
                consumerMap.get(TOPIC).get(0);
        final ConsumerIterator<String, String> iterator = stream.iterator();

        Thread kafkaMessageReceiverThread = new Thread(
                () -> {
                    while (iterator.hasNext()) {
                        String msg = iterator.next().message();
                        msg = msg == null ? "<null>" : msg;
                        System.out.println("got message: " + msg);
                        messagesReceived.add(msg);
                    }
                },
                "kafkaMessageReceiverThread"
        );
        kafkaMessageReceiverThread.start();

    }

}
