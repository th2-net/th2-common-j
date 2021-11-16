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

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.factory.CommonFactory;

public class Test {
    public static void main(String[] args) {
        CommonFactory commonFactory = CommonFactory.createFromArguments("-c", "src/test/resources/test_load_dictionaries");
        testMessageId(commonFactory);
        testEventId(commonFactory);
    }

    public static void testMessageId(CommonFactory commonFactory) {
        MessageID messageId = commonFactory.getMessageIdBuilder()
                .setSessionAlias("alias")
                .setDirection(Direction.FIRST)
                .setSequence(1)
                .addSubsequence(2)
                .setBookName("book")
                .build();
        System.out.println(MessageUtils.toJson(messageId, false));
    }

    public static void testEventId(CommonFactory commonFactory) {
        EventID eventId = commonFactory.getEventIdBuilder()
                .setId("id")
                .setBookName("book")
                .build();
        System.out.println(MessageUtils.toJson(eventId, false));
    }
}
