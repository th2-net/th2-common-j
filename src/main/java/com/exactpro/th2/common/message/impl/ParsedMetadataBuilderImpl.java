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

import com.exactpro.th2.common.grpc.MessageMetadata.Builder;
import com.exactpro.th2.common.message.ParsedMetadataBuilder;

public class ParsedMetadataBuilderImpl
        extends MetadataBuilderImpl<ParsedMetadataBuilderImpl>
        implements ParsedMetadataBuilder {
    private final Builder builder;

    public ParsedMetadataBuilderImpl(Builder builder) {
        super(
                builder.getIdBuilder(),
                builder::setTimestamp,
                builder::putProperties,
                builder::setProtocol
        );
        this.builder = Objects.requireNonNull(builder, "`builder` cannot be null");
    }

    @Override
    public ParsedMetadataBuilder setMessageType(String messageType) {
        builder.setMessageType(messageType);
        return this;
    }

    @Override
    protected ParsedMetadataBuilderImpl builder() {
        return this;
    }
}
