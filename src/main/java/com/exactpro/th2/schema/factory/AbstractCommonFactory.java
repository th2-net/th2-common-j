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
package com.exactpro.th2.schema.factory;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.schema.cradle.CradleConfiguration;
import com.exactpro.th2.schema.exception.CommonFactoryException;
import com.exactpro.th2.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.schema.grpc.router.impl.DefaultGrpcRouter;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.router.impl.ParsedRabbitMessageRouter;
import com.exactpro.th2.schema.message.impl.rabbitmq.router.impl.RawRabbitMessageRouter;
import com.exactpro.th2.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.schema.strategy.route.json.JsonDeserializerRoutingStategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public abstract class AbstractCommonFactory {

    private static ObjectMapper mapper = new ObjectMapper();

    private RabbitMQConfiguration rabbitMQConfiguration = null;
    private MessageRouterConfiguration messageRouterConfiguration = null;
    private GrpcRouterConfiguration grpcRouterConfiguration = null;
    private Class<? extends MessageRouter> messageRouterParsedBatchClass;
    private Class<? extends MessageRouter> messageRouterRawBatchClass;
    private Class<? extends GrpcRouter> grpcRouterClass;

    public AbstractCommonFactory() {
        this(ParsedRabbitMessageRouter.class, RawRabbitMessageRouter.class, DefaultGrpcRouter.class);
    }

    public AbstractCommonFactory(@NotNull Class<? extends MessageRouter> messageRouterParsedBatchClass, @NotNull Class<? extends MessageRouter> messageRouterRawBatchClass, @NotNull Class<? extends GrpcRouter> grpcRouterClass) {
        this.messageRouterParsedBatchClass = messageRouterParsedBatchClass;
        this.messageRouterRawBatchClass = messageRouterRawBatchClass;
        this.grpcRouterClass = grpcRouterClass;
    }

    public MessageRouter<MessageBatch> getMessageRouterParsedBatch() {
        MessageRouter<MessageBatch> router;
        try {
            router = messageRouterParsedBatchClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new CommonFactoryException("Can not create parsed message router", e);
        }
        router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
        return router;
    }

    public MessageRouter<RawMessageBatch> getMessageRouterRawBatch() {
        MessageRouter<RawMessageBatch> router;
        try {
            router = messageRouterRawBatchClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new CommonFactoryException("Can not create raw message router", e);
        }
        router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
        return router;
    }

    public GrpcRouter getGrpcRouter() {
        GrpcRouter router;
        try {
            router = grpcRouterClass.getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new CommonFactoryException("Can not create GRPC router", e);
        }
        router.init(getGrpcRouterConfiguration());
        return router;
    }

    public CradleConfiguration getCradleConfiguration() {
        try (var in = new FileInputStream(getPathToCradleConfiguration().toFile())) {
            return mapper.readerFor(CradleConfiguration.class).readValue(in);
        } catch (IOException e) {
            throw new IllegalStateException("Can not read cradle configuration");
        }
    }

    public <T> T getCustomConfiguration(Class<T> confClass) {
        try (var in = new FileInputStream(getPathToCustomConfiguration().toFile())) {
            return mapper.readerFor(confClass).readValue(in);
        } catch (IOException e) {
            throw new IllegalStateException("Can not read custom configuration");
        }
    }

    protected abstract Path getPathToRabbitMQConfiguration();
    protected abstract Path getPathToMessageRouterConfiguration();
    protected abstract Path getPathToGrpcRouterConfiguration();
    protected abstract Path getPathToCradleConfiguration();
    protected abstract Path getPathToCustomConfiguration();

    protected synchronized RabbitMQConfiguration getRabbitMqConfiguration() {
        if (rabbitMQConfiguration == null) {
            try (var in = new FileInputStream(getPathToRabbitMQConfiguration().toFile())) {
                rabbitMQConfiguration = mapper.readerFor(RabbitMQConfiguration.class).readValue(in);
            } catch (IOException e) {
                throw new IllegalStateException("Can not read rabbit mq configuration", e);
            }
        }

        return rabbitMQConfiguration;
    }

    protected synchronized MessageRouterConfiguration getMessageRouterConfiguration() {
        if (messageRouterConfiguration == null) {
            try (var in = new FileInputStream(getPathToMessageRouterConfiguration().toFile())) {
                messageRouterConfiguration = mapper.readerFor(MessageRouterConfiguration.class).readValue(in);
            } catch (IOException e) {
                throw new IllegalStateException("Can not read message router configuration", e);
            }
        }

        return messageRouterConfiguration;
    }

    protected synchronized GrpcRouterConfiguration getGrpcRouterConfiguration() {
        if (grpcRouterConfiguration == null) {
            var mapper = new ObjectMapper();

            SimpleModule module = new SimpleModule();
            module.addDeserializer(RoutingStrategy.class, new JsonDeserializerRoutingStategy());

            mapper.registerModule(module);

            try (var in = new FileInputStream(getPathToGrpcRouterConfiguration().toFile())) {
                grpcRouterConfiguration = mapper.readerFor(GrpcRouterConfiguration.class).readValue(in);
            } catch (IOException e) {
                throw new IllegalStateException("Can not read grpc router configuration", e);
            }
        }

        return grpcRouterConfiguration;
    }

}
