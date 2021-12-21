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

import java.time.Instant;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.common.event.bean.Message;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.grpc.EventID;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.protobuf.Timestamp;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

@SuppressWarnings("ClassNamePrefixedWithPackageName")
public class EventUtils {
    public static final NoArgGenerator TIME_BASED_UUID_GENERATOR = Generators.timeBasedGenerator();
    public static final String DEFAULT_SCOPE = "th2-scope";

    public static String generateUUID() {
        return TIME_BASED_UUID_GENERATOR.generate().toString();
    }

    public static Message createMessageBean(String text) {
        return new MessageBuilder().text(text).build();
    }

    @Contract("_, _, null -> !null; _, _, !null -> !null")
    public static @NotNull EventID toEventID(
            @NotNull Instant startTimestamp,
            @NotNull String bookName,
            @Nullable String id
    ) {
        return internalToEventID(
                startTimestamp,
                requireNonBlankBookName(bookName),
                DEFAULT_SCOPE,
                id
        );
    }

    @Contract("_, _, _, null -> !null; _, _, _, !null -> !null")
    public static @NotNull EventID toEventID(
            @NotNull Instant startTimestamp,
            @NotNull String bookName,
            @NotNull String scope,
            @Nullable String id
    ) {
        return internalToEventID(
                startTimestamp,
                requireNonBlankBookName(bookName),
                requireNonBlankScope(scope),
                id
        );
    }

    private static @NotNull EventID internalToEventID(
            @NotNull Instant startTimestamp,
            @NotNull String bookName,
            @NotNull String scope,
            @Nullable String id
    ) {
        EventID.Builder builder = EventID
                .newBuilder()
                .setStartTimestamp(toTimestamp(startTimestamp))
                .setBookName(bookName)
                .setScope(scope);
        if (id != null) {
            builder.setId(id);
        }
        return builder.build();
    }

    public static @NotNull Timestamp toTimestamp(@NotNull Instant timestamp) {
        requireNonNullTimestamp(timestamp);
        return Timestamp
                .newBuilder()
                .setSeconds(timestamp.getEpochSecond())
                .setNanos(timestamp.getNano())
                .build();
    }

    public static Instant requireNonNullTimestamp(Instant timestamp) {
        return requireNonNull(timestamp, "Timestamp cannot be null");
    }

    public static EventID requireNonNullParentId(EventID parentId) {
        return requireNonNull(parentId, "Parent id cannot be null");
    }

    public static String requireNonBlankBookName(String bookName) {
        if (isBlank(bookName)) {
            throw new IllegalArgumentException("Book name cannot be null or blank");
        }
        return bookName;
    }

    public static String requireNonBlankScope(String scope) {
        if (isBlank(scope)) {
            throw new IllegalArgumentException("Scope cannot be null or blank");
        }
        return scope;
    }
}
