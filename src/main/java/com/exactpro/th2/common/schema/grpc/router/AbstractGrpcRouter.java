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

import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Abstract implementation for {@link GrpcRouter}
 * <p>
 * Implement {@link GrpcRouter#init(GrpcConfiguration)}
 * <p>
 * Implement {@link GrpcRouter#init(GrpcConfiguration, GrpcRouterConfiguration)}
 * <p>
 * Implement {@link GrpcRouter#startServer(BindableService...)}
 *
 * @see DefaultGrpcRouter
 */
public abstract class AbstractGrpcRouter implements GrpcRouter {
    protected static final long SERVER_SHUTDOWN_TIMEOUT_MS = 5000L;

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGrpcRouter.class);
    protected List<Server> servers = new ArrayList<>();
    protected GrpcConfiguration configuration;
    protected ExecutorService executor;
    protected EventLoopGroup eventLoop;

    @Override
    public void init(GrpcConfiguration configuration) {
        init(configuration, new GrpcRouterConfiguration());
    }

    @Override
    public void init(@NotNull GrpcConfiguration configuration, @NotNull GrpcRouterConfiguration routerConfiguration) {
        throwIsInit();

        this.configuration = Objects.requireNonNull(configuration);
        executor = Executors.newFixedThreadPool(Objects.requireNonNull(routerConfiguration).getWorkers());
        eventLoop = new DefaultEventLoopGroup(routerConfiguration.getWorkers(), executor);
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

        builder.workerEventLoopGroup(eventLoop);

        for (BindableService service : services) {
            builder.addService(service);
        }

        var server = builder.build();

        servers.add(server);

        return server;
    }

    protected void throwIsInit() {
        if (this.configuration != null && eventLoop != null) {
            throw new IllegalStateException("Grpc router already init");
        }
    }

    @Override
    public void close() {
        for (Server server : servers) {
            try {
                LOGGER.info("Shutting down server: {}");
                server.shutdown();

                if (!server.awaitTermination(SERVER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    LOGGER.warn("Failed to shutdown server '{}' in {} ms. Forcing shutdown...", server, SERVER_SHUTDOWN_TIMEOUT_MS);
                    server.shutdownNow();
                }

                LOGGER.info("Server has been successfully shutdown: {}", server);
            } catch (Exception e) {
                LOGGER.error("Failed to shutdown server: {}", server, e);
            }
        }


        LOGGER.info("Shutting down event loop");
        boolean needToForce;
        try {
            needToForce = !eventLoop.shutdownGracefully().await(SERVER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            needToForce = true;
        }
        if (needToForce) {
            LOGGER.warn("Failed to shutdown event loop in {} ms. Forcing shutdown...", SERVER_SHUTDOWN_TIMEOUT_MS);
            eventLoop.shutdownNow();
        }

        needToForce = false;
        try {
            executor.shutdown();
            needToForce = !executor.awaitTermination(SERVER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            needToForce = true;
        }

        if (needToForce) {
            LOGGER.warn("Failed to shutdown executor in {} ms. Forcing shutdown...", SERVER_SHUTDOWN_TIMEOUT_MS);
            executor.shutdownNow();
        }
    }
}
