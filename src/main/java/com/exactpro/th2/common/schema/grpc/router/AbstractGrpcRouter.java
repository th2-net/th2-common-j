/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.router.MethodDetails;
import com.exactpro.th2.common.grpc.router.ServerGrpcInterceptor;
import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.prometheus.client.Counter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Abstract implementation for {@link GrpcRouter}
 * <p>
 * Implement {@link GrpcRouter#init(GrpcRouterConfiguration)}
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
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("grpc-router-server-pool-%d").build();
    protected final List<Server> servers = new ArrayList<>();
    protected final List<EventExecutorGroup> loopGroups = new ArrayList<>();
    protected final List<ExecutorService> executors = new ArrayList<>();
    protected GrpcRouterConfiguration routerConfiguration;
    protected GrpcConfiguration configuration;

    protected static final Counter GRPC_INVOKE_CALL_TOTAL = Counter.build()
            .name("th2_grpc_invoke_call_total")
            .labelNames(CommonMetrics.TH2_PIN_LABEL, CommonMetrics.GRPC_SERVICE_NAME_LABEL, CommonMetrics.GRPC_METHOD_NAME_LABEL)
            .help("Total number of calling particular gRPC method")
            .register();

    protected static final Map<MethodDetails, Counter.Child> GRPC_INVOKE_CALL_MAP = new ConcurrentHashMap<>();

    protected static final Counter GRPC_INVOKE_CALL_REQUEST_BYTES = Counter.build()
            .name("th2_grpc_invoke_call_request_bytes")
            .labelNames(CommonMetrics.TH2_PIN_LABEL, CommonMetrics.GRPC_SERVICE_NAME_LABEL, CommonMetrics.GRPC_METHOD_NAME_LABEL)
            .help("Number of bytes sent to particular gRPC call")
            .register();

    protected static final Map<MethodDetails, Counter.Child> GRPC_INVOKE_CALL_REQUEST_SIZE_MAP = new ConcurrentHashMap<>();

    protected static final Counter GRPC_INVOKE_CALL_RESPONSE_BYTES = Counter.build()
            .name("th2_grpc_invoke_call_response_bytes")
            .labelNames(CommonMetrics.TH2_PIN_LABEL, CommonMetrics.GRPC_SERVICE_NAME_LABEL, CommonMetrics.GRPC_METHOD_NAME_LABEL)
            .help("Number of bytes sent to particular gRPC call")
            .register();

    protected static final Map<MethodDetails, Counter.Child> GRPC_INVOKE_CALL_RESPONSE_SIZE_MAP = new ConcurrentHashMap<>();

    protected static final Counter GRPC_RECEIVE_CALL_TOTAL = Counter.build()
            .name("th2_grpc_receive_call_total")
            .labelNames(CommonMetrics.TH2_PIN_LABEL, CommonMetrics.GRPC_SERVICE_NAME_LABEL, CommonMetrics.GRPC_METHOD_NAME_LABEL)
            .help("Total number of consuming particular gRPC method")
            .register();

    protected static final Map<MethodDetails, Counter.Child> GRPC_RECEIVE_CALL_MAP = new ConcurrentHashMap<>();

    protected static final Counter GRPC_RECEIVE_CALL_REQUEST_BYTES = Counter.build()
            .name("th2_grpc_receive_call_request_bytes")
            .labelNames(CommonMetrics.TH2_PIN_LABEL, CommonMetrics.GRPC_SERVICE_NAME_LABEL, CommonMetrics.GRPC_METHOD_NAME_LABEL)
            .help("Number of bytes received from particular gRPC call")
            .register();

    protected static final Map<MethodDetails, Counter.Child> GRPC_RECEIVE_CALL_REQUEST_SIZE_MAP = new ConcurrentHashMap<>();

    protected static final Counter GRPC_RECEIVE_CALL_RESPONSE_BYTES = Counter.build()
            .name("th2_grpc_receive_call_response_bytes")
            .labelNames(CommonMetrics.TH2_PIN_LABEL, CommonMetrics.GRPC_SERVICE_NAME_LABEL, CommonMetrics.GRPC_METHOD_NAME_LABEL)
            .help("Number of bytes sent to particular gRPC call")
            .register();

    protected static final Map<MethodDetails, Counter.Child> GRPC_RECEIVE_CALL_RESPONSE_SIZE_MAP = new ConcurrentHashMap<>();

    @Override
    public void init(GrpcRouterConfiguration configuration) {
        init(new GrpcConfiguration(), configuration);
    }

    @Override
    public void init(@NotNull GrpcConfiguration configuration, @NotNull GrpcRouterConfiguration routerConfiguration) {
        failIfInitialized();

        this.routerConfiguration = Objects.requireNonNull(routerConfiguration);
        this.configuration = Objects.requireNonNull(configuration);
    }

    @Override
    public Server startServer(BindableService... services) {
        var serverConf = configuration.getServerConfiguration();

        NettyServerBuilder builder;

        if (serverConf.getHost() == null) {
            builder = NettyServerBuilder.forPort(serverConf.getPort());
        } else {
            builder = NettyServerBuilder.forAddress(new InetSocketAddress(serverConf.getHost(), serverConf.getPort()));
        }

        var executor = Executors.newFixedThreadPool(serverConf.getWorkers(), THREAD_FACTORY);
        var eventLoop = new NioEventLoopGroup(serverConf.getWorkers(), executor);

        // Boss event loop - for I/O
        // Worker event loop - for custom logic
        builder = builder.workerEventLoopGroup(eventLoop)
                .bossEventLoopGroup(eventLoop)
                .channelType(NioServerSocketChannel.class)
                .keepAliveTimeout(routerConfiguration.getKeepAliveInterval(), TimeUnit.SECONDS)
                .maxInboundMessageSize(routerConfiguration.getMaxMessageSize())
                .intercept(new ServerGrpcInterceptor("server",
                        createGetMetric(GRPC_INVOKE_CALL_TOTAL, GRPC_INVOKE_CALL_MAP),
                        createGetMetric(GRPC_RECEIVE_CALL_TOTAL, GRPC_RECEIVE_CALL_MAP),
                        createGetMeasuringMetric(GRPC_RECEIVE_CALL_REQUEST_BYTES, GRPC_RECEIVE_CALL_REQUEST_SIZE_MAP),
                        createGetMeasuringMetric(GRPC_RECEIVE_CALL_RESPONSE_BYTES, GRPC_RECEIVE_CALL_RESPONSE_SIZE_MAP)));

        builder.addService(ProtoReflectionService.newInstance());

        for (BindableService service : services) {
            builder.addService(service);
        }

        var server = builder.build();

        executors.add(executor);
        loopGroups.add(eventLoop);
        servers.add(server);

        LOGGER.info("Made gRPC server: host {}, port {}, keepAliveTime {}, max inbound message {}", serverConf.getHost(), serverConf.getPort(), routerConfiguration.getKeepAliveInterval(), routerConfiguration.getMaxMessageSize());

        return server;
    }

    @Override
    public void close() {
        for (Server server : servers) {
            try {
                LOGGER.info("Shutting down gRPC server");
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

        loopGroups.forEach(EventExecutorGroup::shutdownGracefully);
        loopGroups.forEach(group -> {
            if (!group.terminationFuture().awaitUninterruptibly(SERVER_SHUTDOWN_TIMEOUT_MS)) {
                LOGGER.error("Failed to shutdown event loop '{}' in {} ms.", group, SERVER_SHUTDOWN_TIMEOUT_MS);
            }
        });

        executors.forEach(ExecutorService::shutdown);
        executors.forEach(executor -> {
            try {
                if (!executor.awaitTermination(SERVER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    LOGGER.warn("Failed to shutdown executor service '{}' in {} ms. Forcing shutdown...", executor, SERVER_SHUTDOWN_TIMEOUT_MS);
                    executor.shutdownNow();
                }
            } catch (Exception e) {
                LOGGER.error("Failed to shutdown executor service: {}", executor, e);
            }
        });
    }

    protected Function<MethodDetails, Counter.Child> createGetMetric(Counter counter, Map<MethodDetails, Counter.Child> map) {
        return (MethodDetails methodDetails) -> map
                .computeIfAbsent(methodDetails,
                        (key) -> counter
                                .labels(methodDetails.getPinName(),
                                        methodDetails.getServiceName(),
                                        methodDetails.getMethodName()));
    }

    protected Function<MethodDetails, Counter.Child> createGetMeasuringMetric(Counter counter, Map<MethodDetails, Counter.Child> map) {
        if (routerConfiguration.getEnableSizeMeasuring()) {
            return createGetMetric(counter, map);
        }
        return (MethodDetails) -> null;
    }

    protected void failIfInitialized() {
        if (this.configuration != null) {
            throw new IllegalStateException("Grpc router already init");
        }
    }
}