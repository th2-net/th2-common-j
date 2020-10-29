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

package com.exactpro.th2.common.schema.grpc.router.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.proto.service.generator.core.antlr.annotation.GrpcStub;
import com.exactpro.th2.common.schema.exception.InitGrpcRouterException;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.common.schema.grpc.router.AbstractGrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.google.protobuf.Message;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;

/**
 * Default implementation for {@link GrpcRouter}
 * <p>
 * Extends from {@link AbstractGrpcRouter}
 */
public class DefaultGrpcRouter extends AbstractGrpcRouter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGrpcRouter.class);

    private Map<Class<?>, Map<String, AbstractStub<?>>> stubs = new ConcurrentHashMap<>();
    private Map<String, Channel> channels = new ConcurrentHashMap<>();

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

    protected AbstractStub<?> getGrpcStubToSend(Class<?> proxyService, Message message) throws ClassNotFoundException {
        var grpcStubAnn = proxyService.getAnnotation(GrpcStub.class);

        if (Objects.isNull(grpcStubAnn)) {
            throw new ClassNotFoundException("Provided service class not annotated " +
                    "by GrpcStub annotation: " + proxyService.getSimpleName());
        }

        return getStubInstanceOrCreate(proxyService, grpcStubAnn.value(), message);
    }

    protected  <T extends AbstractStub> AbstractStub getStubInstanceOrCreate(Class<?> proxyService, Class<T> stubClass, Message message) {
        var serviceConfig = getServiceConfig(proxyService);

        String endpointName = serviceConfig.getStrategy().getEndpoint(message);

        return stubs.computeIfAbsent(stubClass, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(endpointName, key ->
                        createStubInstance(stubClass, getOrCreateChannel(key, serviceConfig)));
    }

    protected GrpcServiceConfiguration getServiceConfig(Class<?> proxyService) {
        return configuration.getServices().values().stream()
                .filter(sConfig -> {

                    String proxyClassName = proxyService.getName();
                    if (proxyService.getSimpleName().startsWith("Async")) {
                        int index = proxyClassName.lastIndexOf("Async");
                        proxyClassName = proxyClassName.substring(0, index) + proxyClassName.substring(index + 5);
                    }

                    return sConfig.getServiceClass().getName().equals(proxyClassName);
                })
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No services matching the provided " +
                        "class were found in the configuration: " + proxyService.getName()));
    }

    protected Channel getOrCreateChannel(String endpointName, GrpcServiceConfiguration serviceConfig) {
        return channels.computeIfAbsent(endpointName, key -> {
            var grpcServer = serviceConfig.getEndpoints().get(key);

            if (Objects.isNull(grpcServer)) {
                throw new IllegalStateException("No endpoint in configuration " +
                        "that matching the provided alias: " + key);
            }

            return ManagedChannelBuilder.forAddress(grpcServer.getHost(), grpcServer.getPort()).usePlaintext().build();
        });
    }

    @SuppressWarnings("rawtypes")
    protected <T extends AbstractStub> AbstractStub createStubInstance(Class<T> stubClass, Channel channel) {
        try {
            var constr = stubClass.getDeclaredConstructor(Channel.class, CallOptions.class);
            constr.setAccessible(true);
            return constr.newInstance(channel, CallOptions.DEFAULT);
        } catch (NoSuchMethodException e) {
            throw new InitGrpcRouterException("Could not find constructor " +
                    "'(Channel,CallOptions)' in the provided stub class: " + stubClass, e);
        } catch (Exception e) {
            throw new InitGrpcRouterException("Something went wrong while creating stub instance: " + stubClass, e);
        }
    }

}
