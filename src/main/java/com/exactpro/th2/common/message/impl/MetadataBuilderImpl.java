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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.Direction;
import com.google.protobuf.Timestamp;

public abstract class MetadataBuilderImpl<Builder extends MetadataBuilderImpl<Builder>> {
    private final MessageID.Builder idBuilder;
    private final Consumer<Timestamp> timestampSetup;
    private final BiConsumer<String, String> propertySetup;
    private final Consumer<String> protocolSetup;

    protected MetadataBuilderImpl(
            MessageID.Builder idBuilder,
            Consumer<Timestamp> timestampSetup,
            BiConsumer<String, String> propertySetup,
            Consumer<String> protocolSetup
    ) {
        this.idBuilder = Objects.requireNonNull(idBuilder, "`idBuilder` cannot be null");
        this.timestampSetup = Objects.requireNonNull(timestampSetup, "`timestampSetup` cannot be null");
        this.propertySetup = Objects.requireNonNull(propertySetup, "`propertySetup` cannot be null");
        this.protocolSetup = Objects.requireNonNull(protocolSetup, "`protocolSetup` cannot be null");
    }

    protected abstract Builder builder();

    public Builder setSessionAlias(String alias) {
        idBuilder.getConnectionIdBuilder().setSessionAlias(alias);
        return builder();
    }

    public Builder setDirection(Direction direction) {
        idBuilder.setDirection(com.exactpro.th2.common.grpc.Direction.forNumber(direction.getValue()));
        return builder();
    }

    public Builder setSequence(long sequence) {
        idBuilder.setSequence(sequence);
        return builder();
    }

    public Builder addSubsequence(int subSequence) {
        idBuilder.addSubsequence(subSequence);
        return builder();
    }

    public Builder setBookName(String bookName) {
        idBuilder.setBookName(bookName);
        return builder();
    }

    public Builder setTimestamp(Instant timestamp) {
        timestampSetup.accept(Timestamp.newBuilder()
                .setSeconds(timestamp.getEpochSecond())
                .setNanos(timestamp.getNano()).build()
        );
        return builder();
    }

    public Builder putProperty(String key, String value) {
        propertySetup.accept(key, value);
        return builder();
    }

    public Builder setProtocol(String protocol) {
        protocolSetup.accept(protocol);
        return builder();
    }
}
