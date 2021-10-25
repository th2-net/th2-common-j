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

package com.exactpro.th2.common.message;

import java.time.Instant;
import java.util.List;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.event.IEventFactory;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.impl.ParsedMessageBuilderImpl;
import com.exactpro.th2.common.message.impl.RawMessageBuilderImpl;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;

public class Test {
    public static void main(String[] args) throws JsonProcessingException {
        CommonFactory commonFactory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries");
        MessageFactory messageFactory = commonFactory.getMessageFactory();
        String bookName = commonFactory.getBoxConfiguration().getBookName();
        testParsedMessage(messageFactory, bookName);
        testRawMessage(messageFactory, bookName);
        testEvent(commonFactory.getEventFactory(), bookName);
    }

    public static void testParsedMessage(MessageFactory factory, String bookName) {
        ParsedMessageBuilderImpl message = factory.createParsedMessage()
                .setParentEventId("parentEventId", bookName);
        message.metadataBuilder()
                .setSessionAlias("sessionAlias")
                .setDirection(Direction.SECOND)
                .setSequence(1L)
                .addSubsequence(2)
                .addSubsequence(3)
                .setBookName(bookName)
                .setTimestamp(Instant.now())
                .setMessageType("messageType")
                .putProperty("propertyKey1", "propertyValue1")
                .putProperty("propertyKey2", "propertyValue2")
                .setProtocol("protocol");
        message.putSimpleField("A", 5)
                .putSimpleField("B", List.of(5, 42))
                // sets message to a field
                .putMessage("C", msg ->
                        msg.putSimpleField("A", 42)
                                .putSimpleField("B", List.of(42, 3))
                                .putMessage("C", inner ->
                                        inner.putSimpleField("A", 5)
                                                .putSimpleField("B", 4)
                                )
                                .putMessages("D", List.of(
                                        inner -> inner.putSimpleField("A", 1),
                                        inner -> inner.putSimpleField("A", 2),
                                        inner -> inner.putSimpleField("A", 3)
                                ))
                )

                // adds message to a collection for the field
                .addMessage("D", msg -> msg.putSimpleField("A", 1))
                .addMessage("D", msg -> msg.putSimpleField("A", 2))
                .addMessage("D", msg -> msg.putSimpleField("A", 3))

                // sets collection to the field
                .putMessages("E", List.of(
                        msg -> msg.putSimpleField("A", 1),
                        msg -> msg.putSimpleField("A", 2)
                ));
        System.out.println(MessageUtils.toJson(message.build(), false));
    }

    public static void testRawMessage(MessageFactory factory, String bookName) {
        RawMessageBuilderImpl message = factory.createRawMessage()
                .setParentEventId("parentEventId", bookName)
                .setBody("body".getBytes());
        message.metadataBuilder()
                .setSessionAlias("sessionAlias")
                .setDirection(Direction.SECOND)
                .setSequence(1L)
                .addSubsequence(2)
                .addSubsequence(3)
                .setBookName(bookName)
                .setTimestamp(Instant.now())
                .putProperty("propertyKey1", "propertyValue1")
                .putProperty("propertyKey2", "propertyValue2")
                .setProtocol("protocol");
        System.out.println(MessageUtils.toJson(message.build(), false));
    }

    public static void testEvent(IEventFactory eventFactory, String bookName) throws JsonProcessingException {
        com.exactpro.th2.common.grpc.Event event = Event.start(eventFactory)
                .bookName(bookName)
                .status(Event.Status.PASSED)
                .name("name")
                .type("type")
                .bodyData(EventUtils.createMessageBean("bodyData"))
                .messageID(MessageID.newBuilder().build())
                .toProto(null);
        System.out.println(MessageUtils.toJson(event, false));
    }
}
