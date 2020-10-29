/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package com.exactpro.th2.common.schema.grpc.router;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;

import io.grpc.BindableService;
import io.grpc.Server;

/**
 * Interface for create service for manage connections
 * @see AbstractGrpcRouter
 */
public interface GrpcRouter extends AutoCloseable {
    /**
     * Initialization router
     * @param configuration
     */
    void init(GrpcRouterConfiguration configuration);

    /**
     * Create grpc service for send message to grpc servers
     * @param cls service class
     * @return service
     * @throws ClassNotFoundException
     */
    <T> T getService(@NotNull Class<T> cls) throws ClassNotFoundException;

    /**
     * Start server of service
     * @param services server services
     * @return Grpc server
     */
    Server startServer(BindableService... services);

}
