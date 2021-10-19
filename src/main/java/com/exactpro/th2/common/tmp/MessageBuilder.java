package com.exactpro.th2.common.tmp;

public interface MessageBuilder<R> extends MessageBodyBuilder {
    MetadataBuilder metadataBuilder();

    R build();
}
