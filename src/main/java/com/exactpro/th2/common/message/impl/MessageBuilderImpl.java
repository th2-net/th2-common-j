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

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.EventID;

public abstract class MessageBuilderImpl<Builder extends MessageBuilderImpl<Builder, MetadataBuilder, Message>, MetadataBuilder, Message> {
    private final Consumer<EventID> parentEventIdSetup;
    private final MetadataBuilder metadataBuilder;
    private final Supplier<Message> buildSetup;

    protected MessageBuilderImpl(
            Consumer<EventID> parentEventIdSetup,
            MetadataBuilder metadataBuilder,
            Supplier<Message> buildSetup
    ) {
        this.parentEventIdSetup = parentEventIdSetup;
        this.metadataBuilder = metadataBuilder;
        this.buildSetup = buildSetup;
    }

    protected abstract Builder builder();

    public Builder setParentEventId(String id, String bookName) {
        parentEventIdSetup.accept(EventUtils.toEventID(id, bookName));
        return builder();
    }

    public MetadataBuilder metadataBuilder() {
        return metadataBuilder;
    }

    public Message build() {
        return buildSetup.get();
    }
}
