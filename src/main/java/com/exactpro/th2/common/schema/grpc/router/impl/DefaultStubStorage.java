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

import com.exactpro.th2.common.grpc.router.ClientGrpcInterceptor;
import com.exactpro.th2.common.grpc.router.MethodDetails;
import com.exactpro.th2.common.schema.filter.strategy.impl.FieldValueChecker;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration;
import com.exactpro.th2.service.StubStorage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.prometheus.client.Counter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DefaultStubStorage<T extends AbstractStub<T>> implements StubStorage<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStubStorage.class);

    private static class ServiceHolder<T> {
        final String pinName;
        GrpcServiceConfiguration serviceConfig;
        Map<String, T> stubs = new ConcurrentHashMap<>();

        ServiceHolder(String pinName, GrpcServiceConfiguration serviceConfig) {
            this.pinName = pinName;
            this.serviceConfig = serviceConfig;
        }
    }

    private final List<ServiceHolder<T>> services;
    private final @NotNull GrpcRouterConfiguration routerConfiguration;
    private final @NotNull GrpcConfiguration configuration;
    private final @NotNull Function<MethodDetails, Counter.Child> methodInvokeCounter;
    private final @NotNull Function<MethodDetails, Counter.Child> methodReceiveCounter;
    private final @NotNull Function<MethodDetails, Counter.Child> requestBytesCounter;
    private final @NotNull Function<MethodDetails, Counter.Child> responseBytesCounter;


    public DefaultStubStorage(
            @NotNull List<Map.Entry<String, GrpcServiceConfiguration>> serviceConfigurations,
            @NotNull Function<MethodDetails, Counter.Child> methodInvokeCounter,
            @NotNull Function<MethodDetails, Counter.Child> methodReceiveCounter,
            @NotNull Function<MethodDetails, Counter.Child> requestBytesCounter,
            @NotNull Function<MethodDetails, Counter.Child> responseBytesCounter,
            @NotNull GrpcRouterConfiguration routerConfiguration,
            @NotNull GrpcConfiguration configuration
            ) {
        this.methodInvokeCounter = requireNonNull(methodInvokeCounter);
        this.methodReceiveCounter = requireNonNull(methodReceiveCounter);
        this.requestBytesCounter = requireNonNull(requestBytesCounter);
        this.responseBytesCounter = requireNonNull(responseBytesCounter);
        this.routerConfiguration = requireNonNull(routerConfiguration);
        this.configuration = requireNonNull(configuration);

        services = new ArrayList<>(serviceConfigurations.size());
        for (final var config: serviceConfigurations) {
            services.add(new ServiceHolder<>(config.getKey(), config.getValue()));
        }
    }

    @NotNull
    @Override
    public T getStub(@NotNull Message message, @NotNull AbstractStub.StubFactory<T> stubFactory) {
        return getStub(message, stubFactory, Collections.emptyMap());
    }

    @NotNull
    @Override
    public T getStub(@NotNull Message message, @NotNull AbstractStub.StubFactory<T> stubFactory, @NotNull Map<String, String> properties) {

        final var matchingServices = services.stream()
                .filter(service -> service.serviceConfig.getFilters().isEmpty()
                        || service.serviceConfig.getFilters().stream().anyMatch(it -> isAllPropertiesMatch(it.getProperties(), properties)))
                .limit(2)
                .collect(Collectors.toList());

        if(matchingServices.isEmpty()) {
            throw new IllegalStateException("No gRPC pin matches the provided properties: " + properties);
        }

        if(matchingServices.size() > 1) {
            throw new IllegalStateException("More than one gRPC pins match the provided properties: " + properties);
        }

        final var service = matchingServices.get(0);
        final var endpointLabel = service.serviceConfig.getStrategy().getEndpoint(message);

        return service.stubs.computeIfAbsent(endpointLabel, key -> {
            GrpcEndpointConfiguration endpoint = service.serviceConfig.getEndpoints().get(key);

            if (Objects.isNull(endpoint)) {
                throw new IllegalStateException("No endpoint in the configuration " +
                        "that matches the provided alias: " + key);
            }

            LOGGER.info("Made gRPC stub: host {}, port {}, keepAliveTime {}, max inbound message {}", endpoint.getHost(), endpoint.getPort(), routerConfiguration.getKeepAliveInterval(), configuration.getMaxMessageSize());

            return stubFactory.newStub(
                    ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort())
                            .usePlaintext()
                            .intercept(new ClientGrpcInterceptor(service.pinName,
                                    methodInvokeCounter,
                                    methodReceiveCounter,
                                    requestBytesCounter,
                                    responseBytesCounter))
                            .keepAliveTimeout(routerConfiguration.getKeepAliveInterval(), TimeUnit.SECONDS)
                            .maxInboundMessageSize(configuration.getMaxMessageSize())
                            .build(),
                    CallOptions.DEFAULT
            );
        });
    }

    private boolean isAllPropertiesMatch(List<FieldFilterConfiguration> filterProp, Map<String, String> properties) {
        return filterProp.stream().allMatch(it -> FieldValueChecker.checkFieldValue(it, properties.get(it.getFieldName())));
    }
}