package com.exactpro.th2.common.tmp.impl;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Value;
import com.exactpro.th2.common.tmp.MessageBodyBuilder;
import com.exactpro.th2.common.tmp.MessageBuilder;
import com.exactpro.th2.common.tmp.MetadataBuilder;

import static com.exactpro.th2.common.value.ValueUtils.listValue;
import static com.exactpro.th2.common.value.ValueUtils.toValue;

public class ParsedMessageBuilder implements MessageBuilder<Message> {
    private final Message.Builder messageBuilder = Message.newBuilder();
    private final MetadataBuilder metadataBuilder = new ParsedMetadataBuilderImpl(messageBuilder.getMetadataBuilder());

    public ParsedMessageBuilder putSimpleField(String name, Object value) {
        messageBuilder.putFields(name, toValue(value));
        return this;
    }

    public ParsedMessageBuilder putSimpleField(String name, Collection<Object> value) {
        messageBuilder.putFields(name, toValue(value));
        return this;
    }

    public ParsedMessageBuilder putMessage(String name, Consumer<MessageBodyBuilder> setup) {
        Message message = createSubMessage(setup);
        messageBuilder.putFields(name, toValue(message));
        return this;
    }

    public ParsedMessageBuilder addMessage(String name, Consumer<MessageBodyBuilder> setup) {
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

    public ParsedMessageBuilder putMessages(String name, Collection<Consumer<MessageBodyBuilder>> setup) {
        messageBuilder.putFields(name, toValue(listValue().addAllValues(setup.stream().map(ParsedMessageBuilder::createSubMessageValue).collect(Collectors.toList()))));
        return this;
    }

    @Override
    public MetadataBuilder metadataBuilder() {
        return metadataBuilder;
    }

    @Override
    public Message build() {
        return messageBuilder.build();
    }

    private static Message createSubMessage(Consumer<MessageBodyBuilder> setup) {
        var builder = new ParsedMessageBuilder();
        setup.accept(builder);
        return builder.build();
    }

    private static Value createSubMessageValue(Consumer<MessageBodyBuilder> setup) {
        return toValue(createSubMessage(setup));
    }
}
