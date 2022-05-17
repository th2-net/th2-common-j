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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.factory.CommonFactory;

import static com.exactpro.th2.common.message.MessageUtils.toJson;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MessageEventIdBuildersTest {
    private static final String CONFIG_DIRECTORY = "src/test/resources/test_message_event_id_builders";
    private static final String CUSTOM_BOOK = "custom_book";
    private static final String DEFAULT_ALIAS = "alias";
    private static final String DEFAULT_GROUP = "group";
    private static final Direction DEFAULT_DIRECTION = Direction.FIRST;
    private static final int DEFAULT_SEQUENCE = 1;
    private static final int DEFAULT_SUBSEQUENCE = 2;
    private static final String DEFAULT_ID = "id";
    private static final String DEFAULT_SCOPE = "scope";

    private CommonFactory commonFactory;

    @AfterEach
    void tearDown() {
        commonFactory.close();
    }

    @Test
    public void testWithDefaultBookName() {
        commonFactory = CommonFactory.createFromArguments("-c", "");
        assertIds(commonFactory.getBoxConfiguration().getBookName(), defaultMessageIdBuilder(), defaultEventIdBuilder());
    }

    @Test
    public void testWithConfigBookName() {
        commonFactory = CommonFactory.createFromArguments("-c", CONFIG_DIRECTORY);
        assertIds("config_book", defaultMessageIdBuilder(), defaultEventIdBuilder());
    }

    @Test
    public void testWithBookName() {
        commonFactory = CommonFactory.createFromArguments("-c", CONFIG_DIRECTORY);
        assertIds(CUSTOM_BOOK, defaultMessageIdBuilder().setBookName(CUSTOM_BOOK), defaultEventIdBuilder().setBookName(CUSTOM_BOOK));
    }

    private void assertIds(String bookName, MessageID.Builder messageIdBuilder, EventID.Builder eventIdBuilder) {
        assertEquals(
                "{\n" +
                        "  \"connectionId\": {\n" +
                        "    \"sessionAlias\": \"" + DEFAULT_ALIAS + "\",\n" +
                        "    \"sessionGroup\": \"" + DEFAULT_GROUP + "\"\n" +
                        "  },\n" +
                        "  \"direction\": \"" + DEFAULT_DIRECTION.name() + "\",\n" +
                        "  \"sequence\": \"" + DEFAULT_SEQUENCE + "\",\n" +
                        "  \"subsequence\": [" + DEFAULT_SUBSEQUENCE + "],\n" +
                        "  \"bookName\": \"" + bookName + "\"\n" +
                        "}",
                toJson(messageIdBuilder.build(), false)
        );
        assertEquals(
                "{\n" +
                        "  \"id\": \"" + DEFAULT_ID + "\",\n" +
                        "  \"bookName\": \"" + bookName + "\",\n" +
                        "  \"scope\": \"" + DEFAULT_SCOPE + "\"\n" +
                        "}",
                toJson(eventIdBuilder.build(), false)
        );
    }

    private MessageID.Builder defaultMessageIdBuilder() {
        return commonFactory.newMessageIDBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(DEFAULT_ALIAS).setSessionGroup(DEFAULT_GROUP))
                .setDirection(DEFAULT_DIRECTION)
                .setSequence(DEFAULT_SEQUENCE)
                .addSubsequence(DEFAULT_SUBSEQUENCE);
    }

    private EventID.Builder defaultEventIdBuilder() {
        return commonFactory.newEventIDBuilder()
                .setId(DEFAULT_ID)
                .setScope(DEFAULT_SCOPE);
    }
}
