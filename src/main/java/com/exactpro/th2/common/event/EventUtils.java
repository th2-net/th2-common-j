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
package com.exactpro.th2.common.event;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.common.event.bean.Message;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.grpc.EventID;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;

@SuppressWarnings("ClassNamePrefixedWithPackageName")
public class EventUtils {
    public static final NoArgGenerator TIME_BASED_UUID_GENERATOR = Generators.timeBasedGenerator();

    public static String generateUUID() {
        return TIME_BASED_UUID_GENERATOR.generate().toString();
    }

    public static Message createMessageBean(String text) {
        return new MessageBuilder().text(text).build();
    }

    @Contract("_, null -> null; _, !null -> !null")
    public static @Nullable EventID toEventID(String bookName, @Nullable String id) {
        if (id == null) {
            return null;
        }
        return EventID.newBuilder()
                .setBookName(bookName)
                .setId(id)
                .build();
    }
}
