/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.grpc.router;

import java.util.Set;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import io.grpc.BindableService;
import io.grpc.Server;
import org.jetbrains.annotations.NotNull;

/**
 * Interface used for creating services for managing connections
 * @see AbstractGrpcRouter
 */
public interface GrpcRouter extends AutoCloseable {
    /**
     * Initialization router
     */
    @Deprecated(since = "3.9.0", forRemoval = true)
    void init(GrpcRouterConfiguration configuration);

    void init(@NotNull GrpcConfiguration configuration, @NotNull GrpcRouterConfiguration routerConfiguration);

    /**
     * Create grpc service for sending messages to grpc servers
     * @param cls service class
     * @return service
     */
    <T> T getService(@NotNull Class<T> cls);

    /**
     * Returns the gRPC service for each endpoint specified for passed class
     * @param <T> the type of gRPC service to create
     * @param serviceClass the class of corresponding service
     * @return the service for each endpoint specified in the configuration
     */
    default <T> Set<T> getServices(@NotNull Class<T> serviceClass) {
        throw new UnsupportedOperationException("get services does not support by default");
    }

    /**
     * Start server of service
     * @param services server services
     * @return Grpc server
     */
    Server startServer(BindableService... services);

}
