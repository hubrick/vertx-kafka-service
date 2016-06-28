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

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@DataObject
public class StringKafkaMessage extends AbstractKafkaMessage {

    private String payload;

    public StringKafkaMessage() {
    }

    public StringKafkaMessage(String payload) {
        checkNotNull(payload, "payload must not be null");

        this.payload = payload;
    }

    public StringKafkaMessage(String payload, String partKey) {
        super(partKey);
        checkNotNull(payload, "payload must not be null");

        this.payload = payload;
    }

    public StringKafkaMessage(StringKafkaMessage stringKafkaMessage) {
        super(stringKafkaMessage.getPartKey());
        this.payload = stringKafkaMessage.getPayload();
    }

    public StringKafkaMessage(JsonObject jsonObject) {
        super(jsonObject.getString(PART_KEY));
        this.payload = jsonObject.getString(PAYLOAD);
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public JsonObject toJson() {
        final JsonObject jsonObject = super.asJson();
        if (getPayload() != null) {
            jsonObject.put(PAYLOAD, getPayload());
        }
        return jsonObject;
    }
}
