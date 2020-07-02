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

import com.exactpro.th2.exception.InitGrpcRouterException;
import com.exactpro.th2.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.grpc.router.AbstractGrpcRouter;
import com.exactpro.th2.grpc.router.annotation.GrpcStub;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Objects;


public class DefaultGrpcRouter extends AbstractGrpcRouter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGrpcRouter.class);


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

    protected Channel applyFilter(Class<?> proxyService, Message message) {

        var serviceConfig = configuration.getServices().values().stream()
                .filter(sConfig -> sConfig.getServiceClass().equals(proxyService))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No services matching the provided " +
                        "class were found in the configuration: " + proxyService.getName()));

        var endpoints = serviceConfig.getEndpoints();

        var endpointName = serviceConfig.getStrategy().getEndpoint(message);

        var grpcServer = endpoints.get(endpointName);

        if (Objects.isNull(grpcServer)) {
            throw new IllegalStateException("No endpoint in configuration " +
                    "that matching the provided alias: " + endpointName);
        }

        return ManagedChannelBuilder.forAddress(grpcServer.getHost(), grpcServer.getPort()).usePlaintext().build();

    }

    protected AbstractStub<?> getGrpcStubToSend(Class<?> proxyService, Message message) throws ClassNotFoundException {
        var grpcStubAnn = proxyService.getAnnotation(GrpcStub.class);

        if (Objects.isNull(grpcStubAnn)) {
            throw new ClassNotFoundException("Provided service class not annotated " +
                    "by GrpcStub annotation: " + proxyService.getSimpleName());
        }

        return getStubInstance(grpcStubAnn.value(), applyFilter(proxyService, message));
    }

    @SuppressWarnings("rawtypes")
    protected <T extends AbstractStub> AbstractStub getStubInstance(Class<T> stubClass, Channel channel) {
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
