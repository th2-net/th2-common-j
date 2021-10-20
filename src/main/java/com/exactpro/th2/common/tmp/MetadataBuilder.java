package com.exactpro.th2.common.tmp;

public interface MetadataBuilder {
    MetadataBuilder setSessionAlias(String alias);

    MetadataBuilder setSequence(long sequence);

    MetadataBuilder setDirection(Direction direction);
}
