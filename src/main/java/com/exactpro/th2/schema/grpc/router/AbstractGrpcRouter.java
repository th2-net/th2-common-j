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

package com.exactpro.th2.schema.grpc.router;

import java.net.InetSocketAddress;

import com.exactpro.th2.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.schema.grpc.router.impl.DefaultGrpcRouter;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

/**
 * Abstract implementation for {@link GrpcRouter}
 * <p>
 * Implement {@link GrpcRouter#init(GrpcRouterConfiguration)}
 * <p>
 * Implement {@link GrpcRouter#startServer(BindableService...)}
 *
 * @see DefaultGrpcRouter
 */
public abstract class AbstractGrpcRouter implements GrpcRouter {

    protected GrpcRouterConfiguration configuration;

    @Override
    public void init(GrpcRouterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Server startServer(BindableService... services) {
        var serverConf = configuration.getServerConfiguration();
        if (serverConf == null) {
            throw new IllegalStateException("Can not find server configuration");
        }

        NettyServerBuilder builder;

        if (serverConf.getHost() == null) {
            builder = NettyServerBuilder.forPort(serverConf.getPort());
        } else {
            builder = NettyServerBuilder.forAddress(InetSocketAddress.createUnresolved(serverConf.getHost(), serverConf.getPort()));
        }

        for (BindableService service : services) {
            builder.addService(service);
        }

        return builder.build();
    }
}
