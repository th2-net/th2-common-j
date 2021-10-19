package com.exactpro.th2.common.tmp;

import java.util.Collection;
import java.util.function.Consumer;

public interface MessageBodyBuilder {
    MessageBodyBuilder putSimpleField(String name, Object value);
    MessageBodyBuilder putSimpleField(String name, Collection<Object> value);

    MessageBodyBuilder putMessage(String name, Consumer<MessageBodyBuilder> setup);

    MessageBodyBuilder addMessage(String name, Consumer<MessageBodyBuilder> setup);

    MessageBodyBuilder putMessages(String name, Collection<Consumer<MessageBodyBuilder>> setup);
}
