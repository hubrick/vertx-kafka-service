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
import com.hubrick.vertx.kafka.producer.property.StatsDProperties;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests mod-kafka module with enabled StatsD configuration. The deployment should be successfull and
 * the executor call of StatsD should not fail.
 * <p/>
 * This test sends an event to Vert.x EventBus, then registers a handler to handle that event
 * and send it to Kafka broker, by creating Kafka Producer. It checks that the flow works correctly
 * until the point, where message is sent to Kafka.
 */
@RunWith(VertxUnitRunner.class)
public class KafkaModuleDeployWithStatsdEnabledConfigIntegrationTest extends AbstractProducerTest {

    private static final String ADDRESS = "default-address";
    private static final String MESSAGE = "Test message from KafkaModuleDeployWithStatsdEnabledConfigIT!";
    private static final String TOPIC = "some-topic";

    @Test
    // The deployment should be successfull and StatsD executor call should also be successful
    public void test(TestContext testContext) throws Exception {
        JsonObject config = makeDefaultConfig();
        config.put(KafkaProducerProperties.ADDRESS, ADDRESS);
        config.put(KafkaProducerProperties.DEFAULT_TOPIC, TOPIC);

        JsonObject statsDConfig = new JsonObject();
        statsDConfig.put(StatsDProperties.HOST, "localhost");
        statsDConfig.put(StatsDProperties.PORT, 8125);
        statsDConfig.put(StatsDProperties.PREFIX, "testapp.prefix");
        config.put(KafkaProducerProperties.STATSD, statsDConfig);

        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        deploymentOptions.setConfig(config);
        deploy(testContext, deploymentOptions);

        final Async async = testContext.async();
        try {
            JsonObject jsonObject = new JsonObject();
            final KafkaProducerService kafkaProducerService = KafkaProducerService.createProxy(vertx, ADDRESS);
            kafkaProducerService.sendBytes(new ByteKafkaMessage(Buffer.buffer(MESSAGE.getBytes())), (Handler<AsyncResult<Void>>) message -> {
                if (message.failed()) {
                    testContext.assertTrue(message.cause().getMessage().equals("Failed to update metadata after 5 ms."));
                    async.complete();
                } else {
                    testContext.fail(message.cause());
                }
            });

        } catch (Exception e) {
            testContext.fail(e);
        }
    }
}
