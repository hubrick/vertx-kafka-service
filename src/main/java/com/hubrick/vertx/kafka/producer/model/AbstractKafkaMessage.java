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
package com.hubrick.vertx.kafka.producer.model;

import io.vertx.core.json.JsonObject;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public abstract class AbstractKafkaMessage {

    public static final String PAYLOAD = "payload";
    public static final String PART_KEY = "partKey";

    private String partKey;

    protected AbstractKafkaMessage() {}

    protected AbstractKafkaMessage(String partKey) {
        this.partKey = partKey;
    }

    public String getPartKey() {
        return partKey;
    }

    public void setPartKey(String partKey) {
        this.partKey = partKey;
    }

    public JsonObject asJson() {
        final JsonObject jsonObject = new JsonObject();
        if(partKey != null) {
            jsonObject.put(PART_KEY, partKey);
        }
        return jsonObject;
    }
}
