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

package com.exactpro.th2.common.tmp;

import java.time.Instant;
import java.util.List;

import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.tmp.impl.ParsedMessageBuilderImpl;
import com.exactpro.th2.common.tmp.impl.RawMessageBuilderImpl;

public class Test {
    public static void main(String[] args) {
        MessageFactory factory = new MessageFactory();
        testParsedMessage(factory);
        testRawMessage(factory);
    }

    public static void testParsedMessage(MessageFactory factory) {
        ParsedMessageBuilderImpl message = factory.createParsedMessage()
                .setParentEventId("parentEventId");
        message.metadataBuilder()
                .setSessionAlias("sessionAlias")
                .setDirection(Direction.SECOND)
                .setSequence(1L)
                .addSubsequence(2)
                .addSubsequence(3)
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

    public static void testRawMessage(MessageFactory factory) {
        RawMessageBuilderImpl message = factory.createRawMessage()
                .setParentEventId("parentEventId")
                .setBody("body".getBytes());
        message.metadataBuilder()
                .setSessionAlias("sessionAlias")
                .setDirection(Direction.SECOND)
                .setSequence(1L)
                .addSubsequence(2)
                .addSubsequence(3)
                .setTimestamp(Instant.now())
                .putProperty("propertyKey1", "propertyValue1")
                .putProperty("propertyKey2", "propertyValue2")
                .setProtocol("protocol");
        System.out.println(MessageUtils.toJson(message.build(), false));
    }
}
