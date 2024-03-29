/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.exactpro.th2.common.event.bean;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class MessageTest extends BaseTest {
    @Test
    public void testSerializationMessage() throws IOException {

        Message message = new MessageBuilder().text("My message for report")
                .build();

        com.exactpro.th2.common.grpc.Event event = Event
                .start()
                .bodyData(message)
                .toProto(PARENT_EVENT_ID);

        String expectedJson = "[\n" +
                "  {\n" +
                "    \"type\": \"message\",\n" +
                "    \"data\": \"My message for report\"\n" +
                "  }\n" +
                "]";

        assertCompareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }

    @Test
    public void testSerializationMessageNullBody() {
        Assertions.assertThrows(NullPointerException.class, () -> {
            new MessageBuilder().text(null).build();
        });
    }
}
