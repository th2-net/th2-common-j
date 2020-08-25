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

package com.exactpro.th2.schema.factory;

import static com.exactpro.th2.schema.util.ArchiveUtils.getGzipBase64StringDecoder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.schema.cradle.CradleConfiguration;
import com.exactpro.th2.schema.dictionary.DictionaryType;
import com.exactpro.th2.schema.exception.CommonFactoryException;
import com.exactpro.th2.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.schema.grpc.router.impl.DefaultGrpcRouter;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter;
import com.exactpro.th2.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter;
import com.exactpro.th2.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.schema.strategy.route.json.JsonDeserializerRoutingStategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Class for load <b>JSON</b> schema configuration and create {@link GrpcRouter} and {@link MessageRouter}
 *
 * @see CommonFactory
 */
public abstract class AbstractCommonFactory {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RabbitMQConfiguration rabbitMQConfiguration = null;
    private MessageRouterConfiguration messageRouterConfiguration = null;
    private GrpcRouterConfiguration grpcRouterConfiguration = null;
    private final Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass;
    private final Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass;
    private final Class<? extends GrpcRouter> grpcRouterClass;

    /**
     * Create factory with default implementation schema classes
     */
    public AbstractCommonFactory() {
        this(RabbitParsedBatchRouter.class, RabbitRawBatchRouter.class, DefaultGrpcRouter.class);
    }

    /**
     * Create factory with not-default implementations schema classes
     *
     * @param messageRouterParsedBatchClass Class for {@link MessageRouter} which work with {@link MessageBatch}
     * @param messageRouterRawBatchClass    Class for {@link MessageRouter} which work with {@link RawMessageBatch}
     * @param grpcRouterClass               Class for {@link GrpcRouter}
     */
    public AbstractCommonFactory(@NotNull Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass, @NotNull Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass, @NotNull Class<? extends GrpcRouter> grpcRouterClass) {
        this.messageRouterParsedBatchClass = messageRouterParsedBatchClass;
        this.messageRouterRawBatchClass = messageRouterRawBatchClass;
        this.grpcRouterClass = grpcRouterClass;
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link MessageBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
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

    /**
     * @return Initialized {@link MessageRouter} which work with {@link RawMessageBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
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

    /**
     * @return Initialized {@link GrpcRouter}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
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

    /**
     * @return Schema cradle configuration
     * @throws IllegalStateException if can not read configuration
     */
    public CradleConfiguration getCradleConfiguration() {
        try (var in = new FileInputStream(getPathToCradleConfiguration().toFile())) {
            return MAPPER.readerFor(CradleConfiguration.class).readValue(in);
        } catch (IOException e) {
            throw new IllegalStateException("Can not read cradle configuration", e);
        }
    }

    /**
     * Parse json file with custom configuration to java bean using custom {@link ObjectMapper} to deserialize file's content.
     *
     * @param confClass          java bean class
     * @param customObjectMapper object mapper to deserialize configuration
     * @return Java bean with custom configuration, or <b>NULL</b> if configuration is not exists and can not call default constructor from java bean class
     * @throws IllegalStateException if can not read configuration
     */
    public <T> T getCustomConfiguration(Class<T> confClass, ObjectMapper customObjectMapper) {
        File configFile = getPathToCustomConfiguration().toFile();
        if (!configFile.exists()) {
            try {
                return confClass.getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                return null;
            }
        }

        try (var in = new FileInputStream(getPathToCustomConfiguration().toFile())) {
            return customObjectMapper.readerFor(confClass).readValue(in);
        } catch (IOException e) {
            throw new IllegalStateException("Can not read custom configuration", e);
        }
    }

    /**
     * Parse json file with custom configuration to java bean. This method uses default {@link ObjectMapper}.
     * If you need custom setting for deserialization use {@link #getCustomConfiguration(Class, ObjectMapper)} method.
     *
     * @param confClass java bean class
     * @return Java bean with custom configuration, or <b>NULL</b> if configuration is not exists and can not call default constructor from java bean class
     * @throws IllegalStateException if can not read configuration
     */
    public <T> T getCustomConfiguration(Class<T> confClass) {
        return getCustomConfiguration(confClass, MAPPER);
    }

    /**
     * @return Dictionary as {@link InputStream}
     * @throws IllegalStateException if can not read dictionary
     */
    public InputStream readDictionary() {
        return readDictionary(DictionaryType.MAIN);
    }

    /**
     * @param dictionaryType desired type of dictionary
     * @return Dictionary as {@link InputStream}
     * @throws IllegalStateException if can not read dictionary
     */
    public InputStream readDictionary(DictionaryType dictionaryType) {

        try {

            var dictionaries = Files.list(getPathToDictionariesDir())
                    .filter(path -> Files.isRegularFile(path) && path.getFileName().toString().contains(dictionaryType.name()))
                    .collect(Collectors.toList());

            if (dictionaries.isEmpty()) {
                throw new IllegalStateException("No dictionary found with type '" + dictionaryType + "'");
            } else if (dictionaries.size() > 1) {
                throw new IllegalStateException("Found several dictionaries satisfying the '" + dictionaryType + "' type");
            }

            var targetDictionary = dictionaries.get(0);

            return new ByteArrayInputStream(getGzipBase64StringDecoder().decode(Files.readString(targetDictionary)));

        } catch (IOException e) {
            throw new IllegalStateException("Can not read dictionary", e);
        }
    }

    /**
     * @return Path to configuration for RabbitMQ connection
     * @see RabbitMQConfiguration
     */
    protected abstract Path getPathToRabbitMQConfiguration();

    /**
     * @return Path to configuration for {@link MessageRouter}
     * @see MessageRouterConfiguration
     */
    protected abstract Path getPathToMessageRouterConfiguration();

    /**
     * @return Path to configuration for {@link GrpcRouter}
     * @see GrpcRouterConfiguration
     */
    protected abstract Path getPathToGrpcRouterConfiguration();

    /**
     * @return Path to configuration for cradle
     * @see CradleConfiguration
     */
    protected abstract Path getPathToCradleConfiguration();

    /**
     * @return Path to custom configuration
     */
    protected abstract Path getPathToCustomConfiguration();

    /**
     * @return Path to dictionary
     */
    protected abstract Path getPathToDictionariesDir();

    protected synchronized RabbitMQConfiguration getRabbitMqConfiguration() {
        if (rabbitMQConfiguration == null) {
            try (var in = new FileInputStream(getPathToRabbitMQConfiguration().toFile())) {
                rabbitMQConfiguration = MAPPER.readerFor(RabbitMQConfiguration.class).readValue(in);
            } catch (IOException e) {
                throw new IllegalStateException("Can not read rabbit mq configuration", e);
            }
        }

        return rabbitMQConfiguration;
    }

    protected synchronized MessageRouterConfiguration getMessageRouterConfiguration() {
        if (messageRouterConfiguration == null) {
            try (var in = new FileInputStream(getPathToMessageRouterConfiguration().toFile())) {
                messageRouterConfiguration = MAPPER.readerFor(MessageRouterConfiguration.class).readValue(in);
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
