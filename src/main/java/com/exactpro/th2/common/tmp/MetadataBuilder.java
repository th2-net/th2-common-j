package com.exactpro.th2.common.tmp;

import com.exactpro.th2.common.grpc.Direction;

public interface MetadataBuilder {
    MetadataBuilder setSessionAlias(String alias);

    MetadataBuilder setSequence(long sequence);

    MetadataBuilder setDirection(Direction direction);
}
