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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@DataObject(generateConverter = true)
public class ByteKafkaMessage extends AbstractKafkaMessage {

    private Buffer payload;

    public ByteKafkaMessage() {
    }

    public ByteKafkaMessage(Buffer payload) {
        checkNotNull(payload, "payload must not be null");

        this.payload = payload;
    }

    public ByteKafkaMessage(Buffer payload, String partKey) {
        super(partKey);
        checkNotNull(payload, "payload must not be null");

        this.payload = payload;
    }

    public ByteKafkaMessage(ByteKafkaMessage byteKafkaMessage) {
        super(byteKafkaMessage.getPartKey());
        checkNotNull(payload, "payload must not be null");

        this.payload = byteKafkaMessage.getPayload();
    }

    public ByteKafkaMessage(JsonObject jsonObject) {
        super(jsonObject.getString(PART_KEY));
        this.payload = jsonObject.getBinary(PAYLOAD) != null ? Buffer.buffer(jsonObject.getBinary(PAYLOAD)) : null;
    }

    public Buffer getPayload() {
        return payload;
    }

    public void setPayload(Buffer payload) {
        this.payload = payload;
    }

    public JsonObject toJson() {
        final JsonObject jsonObject = super.asJson();
        if (getPayload() != null) {
            jsonObject.put(PAYLOAD, payload != null ? payload.getBytes() : null);
        }
        return jsonObject;
    }
}
