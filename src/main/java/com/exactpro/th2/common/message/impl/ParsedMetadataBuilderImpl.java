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

package com.exactpro.th2.common.message.impl;

import java.time.Instant;
import java.util.Objects;

import com.exactpro.th2.common.grpc.MessageMetadata.Builder;
import com.exactpro.th2.common.message.Direction;
import com.exactpro.th2.common.message.ParsedMetadataBuilder;

public class ParsedMetadataBuilderImpl implements ParsedMetadataBuilder {
    private final Builder builder;

    public ParsedMetadataBuilderImpl(Builder builder) {
        this.builder = Objects.requireNonNull(builder, "'Builder' parameter");
    }

    @Override
    public ParsedMetadataBuilder setSessionAlias(String alias) {
        builder.getIdBuilder().getConnectionIdBuilder().setSessionAlias(alias);
        return this;
    }

    @Override
    public ParsedMetadataBuilder setDirection(Direction direction) {
        builder.getIdBuilder().setDirection(com.exactpro.th2.common.grpc.Direction.forNumber(direction.getValue()));
        return this;
    }

    @Override
    public ParsedMetadataBuilder setSequence(long sequence) {
        builder.getIdBuilder().setSequence(sequence);
        return this;
    }

    @Override
    public ParsedMetadataBuilder addSubsequence(int subSequence) {
        builder.getIdBuilder().addSubsequence(subSequence);
        return this;
    }

    @Override
    public ParsedMetadataBuilder setTimestamp(Instant timestamp) {
        builder.setTimestamp(
                builder.getTimestampBuilder()
                        .setSeconds(timestamp.getEpochSecond())
                        .setNanos(timestamp.getNano())
        );
        return this;
    }

    @Override
    public ParsedMetadataBuilder setMessageType(String messageType) {
        builder.setMessageType(messageType);
        return this;
    }

    @Override
    public ParsedMetadataBuilder putProperty(String key, String value) {
        builder.putProperties(key, value);
        return this;
    }

    @Override
    public ParsedMetadataBuilder setProtocol(String protocol) {
        builder.setProtocol(protocol);
        return this;
    }
}
