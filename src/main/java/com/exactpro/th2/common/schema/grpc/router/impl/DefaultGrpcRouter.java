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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcRetryConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.common.schema.grpc.router.AbstractGrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.grpc.service.DefaultImpl;

import io.grpc.Channel;
import io.grpc.stub.AbstractStub;

/**
 * Default implementation for {@link GrpcRouter}
 * <p>
 * Extends from {@link AbstractGrpcRouter}
 */
public class DefaultGrpcRouter extends AbstractGrpcRouter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGrpcRouter.class);

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
        Objects.requireNonNull(proxyService, "Service class can not be null");

        DefaultImpl defaultAnnotation = proxyService.getAnnotation(DefaultImpl.class);
        if (defaultAnnotation == null) {
            throw new IllegalStateException("Can not find default implementation for service " + proxyService);
        }

        Class<?> defaultImplClass = defaultAnnotation.value();

        try {
            return (T) defaultImplClass.getConstructor(GrpcRetryConfiguration.class, GrpcServiceConfiguration.class)
                    .newInstance(configuration.getRetryConfiguration(), getServiceConfig(proxyService));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalStateException("Can not create new service implementation");
        }
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

}
