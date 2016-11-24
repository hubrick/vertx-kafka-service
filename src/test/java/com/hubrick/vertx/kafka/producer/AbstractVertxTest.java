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

import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import kafka.Kafka;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public abstract class AbstractVertxTest {

    protected static String SERVICE_NAME = "service:com.hubrick.services.kafka-producer";
    protected Vertx vertx;

    static JsonObject makeDefaultConfig() {
        JsonObject config = new JsonObject();
        config.put(KafkaProducerProperties.BOOTSTRAP_SERVERS, KafkaProducerProperties.BOOTSTRAP_SERVERS_DEFAULT);
        config.put(KafkaProducerProperties.ACKS, KafkaProducerProperties.ACKS_DEFAULT);
        config.put(KafkaProducerProperties.MAX_BLOCK_MS, 5);
        config.put(KafkaProducerProperties.RETRIES, 3);
        config.put(KafkaProducerProperties.REQUEST_TIMEOUT_MS, 1000);
        return config;
    }

    @Before
    public final void init(TestContext testContext) throws Exception {
        vertx = Vertx.vertx();
    }

    @After
    public final void destroy() throws Exception {
        vertx.close();
    }

    protected void deploy(TestContext testContext, DeploymentOptions deploymentOptions) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        vertx.deployVerticle(SERVICE_NAME, deploymentOptions, result -> {
            if (result.failed()) {
                result.cause().printStackTrace();
                testContext.fail();
            }
            latch.countDown();
        });

        latch.await(30, TimeUnit.SECONDS);
    }
}
