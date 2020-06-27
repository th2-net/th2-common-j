/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.grpc.router.impl;

import com.exactpro.th2.common.message.configuration.FilterConfiguration;
import com.exactpro.th2.exception.InitGrpcRouterException;
import com.exactpro.th2.exception.NoConnectionToSendException;
import com.exactpro.th2.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.grpc.router.AbstractGrpcRouter;
import com.exactpro.th2.grpc.router.strategy.fieldExtraction.FieldExtractionStrategy;
import com.exactpro.th2.grpc.router.strategy.fieldExtraction.impl.Th2MsgFieldExtraction;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


public class DefaultGrpcRouter extends AbstractGrpcRouter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGrpcRouter.class);


    @Getter
    @Setter
    protected Map<Class<?>, Class<? extends AbstractStub>> serviceToStubAccordance;

    protected FieldExtractionStrategy fieldExtStrategy = new Th2MsgFieldExtraction();

    public DefaultGrpcRouter() {
        this.serviceToStubAccordance = configuration.getServices().entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream())
                .map(entry -> Map.entry(entry.getKey(), configuration.getServiceToStubMatch().get(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    /**
     * Creates a service instance according to the filters in {@link GrpcRouterConfiguration}
     *
     * @param cls class of service
     * @param <T> type of service
     * @return service instance
     * @throws ClassNotFoundException if matching the service class to protobuf stub has failed
     */
    public <T> T getService(@NotNull Class<T> cls) throws ClassNotFoundException {
        return getProxyService(cls);
    }

    /**
     * Sets a fields extraction strategy
     *
     * @param fieldExtStrategy strategy for extracting fields for filtering
     * @throws NullPointerException if {@code fieldExtractionStrategy} is null
     */
    public void setFieldExtractionStrategy(FieldExtractionStrategy fieldExtStrategy) {
        Objects.requireNonNull(fieldExtStrategy);
        this.fieldExtStrategy = fieldExtStrategy;
    }

    @SuppressWarnings("unchecked")
    protected <T> T getProxyService(Class<T> proxyService) {
        return (T) Proxy.newProxyInstance(
                proxyService.getClassLoader(),
                new Class[]{proxyService},
                (o, method, args) -> {
                    try {
                        validateArgs(args);

                        var message = (Message) args[0];

                        var stub = getGrpcStubToSend(proxyService, message);

                        return stub.getClass()
                                .getMethod(method.getName(), method.getParameterTypes())
                                .invoke(stub, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
        );
    }

    protected void validateArgs(Object[] args) {

        if (args.length > 0) {
            var firstArg = args[0];
            if (!(firstArg instanceof Message)) {
                throw new IllegalArgumentException("The first argument " +
                        "of the service method should be a protobuf Message");
            }
            if (args.length > 1) {
                var secArg = args[1];
                if (!(secArg instanceof StreamObserver)) {
                    throw new IllegalArgumentException("If the second argument of the " +
                            "service method is specified, then it should be a protobuf StreamObserver");
                }
            }
        } else {
            throw new IllegalArgumentException("At least one argument must be provided to send the request");
        }

    }

    protected Channel applyFilter(Class<?> proxyService, Message message) {

        var msgFields = fieldExtStrategy.getFields(message);

        var servers = configuration.getServers();

        var grpcFilters = configuration.getFilterToServerMatch();

        var serviceAlias = configuration.getServices().entrySet().stream()
                .filter(entry -> Objects.nonNull(entry.getValue().get(proxyService)))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No services matching the provided " +
                        "class were found in the configuration: " + proxyService.getName()));

        var serviceFilters = configuration.getServiceToFiltersMatch().get(serviceAlias);

        var grpcServer = configuration.getFilters().entrySet().stream()
                .filter(entry -> serviceFilters.contains(entry.getKey())
                        && applyFilter(msgFields, entry.getValue().getMessage()))
                .map(entry -> servers.get(grpcFilters.get(entry.getKey())))
                .findFirst()
                .orElseThrow(() ->
                        new NoConnectionToSendException("No grpc connections matching the specified filters")
                );

        return ManagedChannelBuilder.forAddress(grpcServer.getHost(), grpcServer.getPort()).usePlaintext().build();

    }

    protected boolean applyFilter(Map<String, String> messageFields, Map<String, FilterConfiguration> fieldFilters) {
        return fieldFilters.entrySet().stream().allMatch(entry -> {
            var fieldName = entry.getKey();
            var fieldFilter = entry.getValue();
            var msgFieldValue = messageFields.get(fieldName);
            return fieldFilter.checkValue(msgFieldValue);
        });
    }

    protected AbstractStub<?> getGrpcStubToSend(Class<?> proxyService, Message message) throws ClassNotFoundException {
        var stubClass = serviceToStubAccordance.get(proxyService);

        if (Objects.isNull(stubClass)) {
            throw new ClassNotFoundException("No matching protobuf stub was found " +
                    "for the provided service class: " + proxyService.getSimpleName());
        }

        return getStubInstance(stubClass, applyFilter(proxyService, message));
    }

    protected <T extends AbstractStub<T>> AbstractStub<T> getStubInstance(Class<T> stubClass, Channel channel) {
        try {
            var constr = stubClass.getDeclaredConstructor(Channel.class, CallOptions.class);
            constr.setAccessible(true);
            return constr.newInstance(channel, CallOptions.DEFAULT);
        } catch (Exception e) {
            throw new InitGrpcRouterException("Could not find constructor " +
                    "'(Channel,CallOptions)' in the provided stub class " + stubClass, e);
        }
    }

}
