/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration;
import com.exactpro.th2.common.schema.grpc.router.AbstractStubStorage;
import com.google.protobuf.Message;

import io.grpc.stub.AbstractStub;
import io.grpc.stub.AbstractStub.StubFactory;

public class SingleEndpointStubStorage<T extends AbstractStub<T>> extends AbstractStubStorage<T> {
    private final GrpcEndpointConfiguration endpoint;
    private final AtomicReference<T> stub = new AtomicReference<>();

    public SingleEndpointStubStorage(GrpcEndpointConfiguration endpoint) {
        this.endpoint = Objects.requireNonNull(endpoint, "'Endpoint' parameter");
    }

    @NotNull
    @Override
    public T getStub(@NotNull Message message, @NotNull StubFactory<T> stubFactory) {
        return stub.updateAndGet(cur -> cur == null ? createStub(stubFactory, endpoint) : cur);
    }
}
