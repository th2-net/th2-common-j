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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected static final long SERVER_SHUTDOWN_TIMEOUT_MS = 5000L;

    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected List<Server> servers = new ArrayList<>();
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

        var server = builder.build();

        servers.add(server);

        return server;
    }

    @Override
    public void close() {
        for (Server server : servers) {
            try {
                logger.info("Shutting down server: {}");
                server.shutdown();

                if (!server.awaitTermination(SERVER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    logger.warn("Failed to shutdown server '{}' in {} ms. Forcing shutdown...", server, SERVER_SHUTDOWN_TIMEOUT_MS);
                    server.shutdownNow();
                }

                logger.info("Server has been successfully shutdown: {}", server);
            } catch (Exception e) {
                logger.error("Failed to shutdown server: {}", server, e);
            }
        }
    }
}
