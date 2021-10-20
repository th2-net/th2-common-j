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

import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.message.MessageBodyBuilder;
import com.exactpro.th2.common.message.ParsedMessageBuilder;
import com.exactpro.th2.common.message.ParsedMetadataBuilder;

import static com.exactpro.th2.common.value.ValueUtils.listValue;
import static com.exactpro.th2.common.value.ValueUtils.toValue;

public class ParsedMessageBuilderImpl implements ParsedMessageBuilder<Message> {
    private final Message.Builder messageBuilder = Message.newBuilder();
    private final ParsedMetadataBuilder metadataBuilder = new ParsedMetadataBuilderImpl(messageBuilder.getMetadataBuilder());

    @Override
    public ParsedMessageBuilderImpl setParentEventId(String id) {
        messageBuilder.setParentEventId(EventUtils.toEventID(id));
        return this;
    }

    @Override
    public ParsedMetadataBuilder metadataBuilder() {
        return metadataBuilder;
    }

    @Override
    public ParsedMessageBuilderImpl putSimpleField(String name, Object value) {
        messageBuilder.putFields(name, toValue(value));
        return this;
    }

    @Override
    public ParsedMessageBuilderImpl putSimpleField(String name, Collection<Object> value) {
        messageBuilder.putFields(name, toValue(value));
        return this;
    }

    @Override
    public ParsedMessageBuilderImpl putMessage(String name, Consumer<MessageBodyBuilder> setup) {
        Message message = createSubMessage(setup);
        messageBuilder.putFields(name, toValue(message));
        return this;
    }

    @Override
    public ParsedMessageBuilderImpl addMessage(String name, Consumer<MessageBodyBuilder> setup) {
        var oldValue = messageBuilder.getFieldsMap().get(name);
        if (oldValue != null && !oldValue.hasListValue()) {
            throw new IllegalStateException("field " + name + " is not a collection: " + oldValue.getKindCase());
        }
        if (oldValue == null) {
            messageBuilder.putFields(name, toValue(listValue().addValues(toValue(createSubMessage(setup)))));
        } else {
            var builder = oldValue.toBuilder();
            builder.getListValueBuilder().addValues(toValue(createSubMessage(setup)));
            messageBuilder.putFields(name, builder.build());
        }
        return this;
    }

    @Override
    public ParsedMessageBuilderImpl putMessages(String name, Collection<Consumer<MessageBodyBuilder>> setup) {
        messageBuilder.putFields(name, toValue(listValue().addAllValues(setup.stream().map(ParsedMessageBuilderImpl::createSubMessageValue).collect(Collectors.toList()))));
        return this;
    }

    @Override
    public Message build() {
        return messageBuilder.build();
    }

    private static Message createSubMessage(Consumer<MessageBodyBuilder> setup) {
        var builder = new ParsedMessageBuilderImpl();
        setup.accept(builder);
        return builder.build();
    }

    private static Value createSubMessageValue(Consumer<MessageBodyBuilder> setup) {
        return toValue(createSubMessage(setup));
    }
}
