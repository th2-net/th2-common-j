package com.exactpro.th2.common.tmp.impl;

import java.util.Objects;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageMetadata.Builder;
import com.exactpro.th2.common.tmp.MetadataBuilder;

public class ParsedMetadataBuilderImpl implements MetadataBuilder {
    private final Builder builder;

    public ParsedMetadataBuilderImpl(Builder builder) {
        this.builder = Objects.requireNonNull(builder, "'Builder' parameter");
    }

    @Override
    public MetadataBuilder setSessionAlias(String alias) {
        builder.getIdBuilder().getConnectionIdBuilder().setSessionAlias(alias);
        return this;
    }

    @Override
    public MetadataBuilder setSequence(long sequence) {
        builder.getIdBuilder().setSequence(sequence);
        return this;
    }

    @Override
    public MetadataBuilder setDirection(Direction direction) {
        builder.getIdBuilder().setDirection(direction);
        return this;
    }
}
