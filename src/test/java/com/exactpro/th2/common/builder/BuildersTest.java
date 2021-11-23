/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.builder;

import org.junit.jupiter.api.Test;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.factory.CommonFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BuildersTest {
    private final CommonFactory commonFactory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries");

    @Test
    public void testMessageId() {
        MessageID messageId = commonFactory.newMessageIDBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias("alias"))
                .setDirection(Direction.FIRST)
                .setSequence(1)
                .addSubsequence(2)
                .setBookName("book")
                .build();
        assertEquals(
                "{\n" +
                        "  \"connectionId\": {\n" +
                        "    \"sessionAlias\": \"alias\"\n" +
                        "  },\n" +
                        "  \"direction\": \"FIRST\",\n" +
                        "  \"sequence\": \"1\",\n" +
                        "  \"subsequence\": [2],\n" +
                        "  \"bookName\": \"book\"\n" +
                        "}",
                MessageUtils.toJson(messageId, false)
        );
    }

    @Test
    public void testEventId() {
        EventID eventId = commonFactory.newEventIDBuilder()
                .setId("id")
                .setBookName("book")
                .build();
        assertEquals(
                "{\n" +
                        "  \"id\": \"id\",\n" +
                        "  \"bookName\": \"book\"\n" +
                        "}",
                MessageUtils.toJson(eventId, false)
        );
    }
}
