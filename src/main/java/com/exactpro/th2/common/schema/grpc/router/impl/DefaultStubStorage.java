/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.schema.grpc.router.impl;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.service.StubStorage;
import com.google.protobuf.Message;

import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;

@ThreadSafe
public class DefaultStubStorage<T extends AbstractStub<T>> implements StubStorage<T> {

    private final GrpcServiceConfiguration serviceConfiguration;
    private final Map<String, T> stubs = new ConcurrentHashMap<>();

    public DefaultStubStorage(@NotNull GrpcServiceConfiguration serviceConfiguration) {
        this.serviceConfiguration = Objects.requireNonNull(serviceConfiguration, "Service configuration can not be null");
    }

    @NotNull
    @Override
    public T getStub(@NotNull Message message, @NotNull AbstractStub.StubFactory<T> stubFactory, String... attrs) {
        String endpointLabel = serviceConfiguration.getStrategy().getEndpoint(message, serviceConfiguration.getEndpoints(), attrs);
        return stubs.computeIfAbsent(endpointLabel, key -> {
            GrpcEndpointConfiguration endpoint = serviceConfiguration.getEndpoints().get(key);

            if (Objects.isNull(endpoint)) {
                throw new IllegalStateException("No endpoint in the configuration " +
                        "that matches the provided alias: " + key);
            }

            return stubFactory.newStub(ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build(), CallOptions.DEFAULT);
        });
    }
}
