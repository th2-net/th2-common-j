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

import java.util.Objects;

import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.message.RawMessageBuilder;
import com.exactpro.th2.common.message.RawMetadataBuilder;
import com.google.protobuf.ByteString;

public class RawMessageBuilderImpl
        extends MessageBuilderImpl<RawMessageBuilderImpl, RawMetadataBuilder, RawMessage>
        implements RawMessageBuilder<RawMessage> {
    private final RawMessage.Builder messageBuilder;

    public RawMessageBuilderImpl() {
        this(RawMessage.newBuilder());
    }

    private RawMessageBuilderImpl(RawMessage.Builder messageBuilder) {
        super(
                messageBuilder::setParentEventId,
                new RawMetadataBuilderImpl(messageBuilder.getMetadataBuilder()),
                messageBuilder::build
        );
        this.messageBuilder = Objects.requireNonNull(messageBuilder, "`messageBuilder` cannot be null");
    }

    @Override
    public RawMessageBuilderImpl setBody(byte[] bytes) {
        messageBuilder.setBody(ByteString.copyFrom(bytes));
        return this;
    }

    @Override
    protected RawMessageBuilderImpl builder() {
        return this;
    }
}
