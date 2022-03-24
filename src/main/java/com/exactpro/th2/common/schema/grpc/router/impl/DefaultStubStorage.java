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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import com.exactpro.th2.common.schema.grpc.configuration.FieldFilterConfiguration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
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

    private static class ServiceHolder<T> {
        GrpcServiceConfiguration serviceConfig;
        Map<String, T> stubs = new ConcurrentHashMap<>();

        ServiceHolder(GrpcServiceConfiguration serviceConfig) {
            this.serviceConfig = serviceConfig;
        }
    }

    private final List<ServiceHolder<T>> services;

    public DefaultStubStorage(@NotNull List<GrpcServiceConfiguration> serviceConfigurations) {
        services = new ArrayList<>(serviceConfigurations.size());
        for (GrpcServiceConfiguration config: serviceConfigurations) {
            services.add(new ServiceHolder<>(config));
        }
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
            throw new  IllegalStateException("No gRPC pin matches the provided properties");
        }

        if(matchingServices.size() > 1) {
            throw new  IllegalStateException("More than one gRPC pins match the provided properties");
        }

        final var service = matchingServices.get(0);
        final var endpointLabel = service.serviceConfig.getStrategy().getEndpoint(message);

        return service.stubs.computeIfAbsent(endpointLabel, key -> {
            GrpcEndpointConfiguration endpoint = service.serviceConfig.getEndpoints().get(key);

            if (Objects.isNull(endpoint)) {
                throw new IllegalStateException("No endpoint in the configuration " +
                        "that matches the provided alias: " + key);
            }

            return stubFactory.newStub(ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build(), CallOptions.DEFAULT);
        });
    }

    private boolean isAllPropertiesMatch(List<FieldFilterConfiguration> filterProp, Map<String, String> properties) {
        return filterProp.stream().allMatch(it -> checkValue(properties.get(it.getFieldName()), it));
    }

    // FIXME: Code duplication (AbstractFilterStrategy)
    private boolean checkValue(String value, FieldFilterConfiguration filterConfiguration) {
        var valueInConf = filterConfiguration.getExpectedValue();

        // FIXME: Change switch to switch-expression after upping java version
        switch (filterConfiguration.getOperation()) {
            case EQUAL:
                return Objects.equals(value, valueInConf);
            case NOT_EQUAL:
                return !Objects.equals(value, valueInConf);
            case EMPTY:
                return StringUtils.isEmpty(value);
            case NOT_EMPTY:
                return StringUtils.isNotEmpty(value);
            case WILDCARD:
                return FilenameUtils.wildcardMatch(value, valueInConf);
            default:
                return false;
        }
    }
}