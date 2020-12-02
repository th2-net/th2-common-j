package com.exactpro.th2.common.schema.grpc.router.impl;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.service.generator.service.StubStorage;
import com.google.protobuf.Message;

import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;

@ThreadSafe
public class DefaultStubStorage<T extends AbstractStub<T>> implements StubStorage<T> {

    private final GrpcServiceConfiguration serviceConfiguration;
    private final Map<String, T> stubs = new ConcurrentHashMap<>();

    public DefaultStubStorage(GrpcServiceConfiguration serviceConfiguration) {
        this.serviceConfiguration = serviceConfiguration;
    }

    @NotNull
    @Override
    public T getStub(@NotNull Message message, @NotNull AbstractStub.StubFactory<T> stubFactory) {
        String endpointLabel = serviceConfiguration.getStrategy().getEndpoint(message);
        return stubs.computeIfAbsent(endpointLabel, key -> {
            GrpcEndpointConfiguration endpoint = serviceConfiguration.getEndpoints().get(key);

            if (Objects.isNull(endpoint)) {
                throw new IllegalStateException("No endpoint in configuration " +
                        "that matching the provided alias: " + key);
            }

            return stubFactory.newStub(ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build(), CallOptions.DEFAULT);
        });
    }
}
