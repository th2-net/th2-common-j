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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookupFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.configuration.Configuration;
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

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.Getter;

/**
 * Class for load <b>JSON</b> schema configuration and create {@link GrpcRouter} and {@link MessageRouter}
 *
 * @see CommonFactory
 */
public abstract class AbstractCommonFactory implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Logger logger = LoggerFactory.getLogger(getClass());
    @Getter(lazy = true)
    private final RabbitMQConfiguration rabbitMqConfiguration = loadRabbitMqConfiguration();
    @Getter(lazy = true)
    private final MessageRouterConfiguration messageRouterConfiguration = loadMessageRouterConfiguration();
    @Getter(lazy = true)
    private final GrpcRouterConfiguration grpcRouterConfiguration = loadGrpcRouterConfiguration();
    private final Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass;
    private final Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass;
    private final Class<? extends GrpcRouter> grpcRouterClass;
    private final AtomicReference<MessageRouter<MessageBatch>> messageRouterParsedBatch = new AtomicReference<>();
    private final AtomicReference<MessageRouter<RawMessageBatch>> messageRouterRawBatch = new AtomicReference<>();
    private final AtomicReference<GrpcRouter> grpcRouter = new AtomicReference<>();
    private final HTTPServer prometheusExporter;

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

        try {
            DefaultExports.initialize();
            this.prometheusExporter = new HTTPServer(Configuration.getEnvPrometheusHost(), Configuration.getEnvPrometheusPort());
        } catch (IOException e) {
            throw new CommonFactoryException("Failed to create Prometheus exporter", e);
        }
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link MessageBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<MessageBatch> getMessageRouterParsedBatch() {
        return messageRouterParsedBatch.updateAndGet(router -> {
            if (router == null) {
                try {
                    router = messageRouterParsedBatchClass.getConstructor().newInstance();
                    router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create parsed message router", e);
                }
            }

            return router;
        });
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link RawMessageBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<RawMessageBatch> getMessageRouterRawBatch() {
        return messageRouterRawBatch.updateAndGet(router -> {
            if (router == null) {
                try {
                    router = messageRouterRawBatchClass.getConstructor().newInstance();
                    router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create raw message router", e);
                }
            }

            return router;
        });
    }

    /**
     * @return Initialized {@link GrpcRouter}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public GrpcRouter getGrpcRouter() {
        return grpcRouter.updateAndGet(router -> {
            if (router == null) {
                try {
                    router = grpcRouterClass.getConstructor().newInstance();
                    router.init(getGrpcRouterConfiguration());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create GRPC router", e);
                }
            }

            return router;
        });
    }

    /**
     * @return Configuration by specified path
     * @throws IllegalStateException if can not read configuration
     */
    public <T> T getConfiguration(Path configPath, Class<T> configClass, ObjectMapper customObjectMapper) {
        try {
            StringSubstitutor stringSubstitutor = new StringSubstitutor(StringLookupFactory.INSTANCE.environmentVariableStringLookup());
            String contents = stringSubstitutor.replace(new String(Files.readAllBytes(configPath)));
            return customObjectMapper.readerFor(configClass).readValue(contents);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Can not read %s configuration", configClass.getName()), e);
        }
    }

    /**
     * @return Schema cradle configuration
     * @throws IllegalStateException if can not read configuration
     */
    public CradleConfiguration getCradleConfiguration() {
        return getConfiguration(getPathToCradleConfiguration(), CradleConfiguration.class, MAPPER);
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

        return getConfiguration(getPathToCustomConfiguration(), confClass, customObjectMapper);
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

    protected RabbitMQConfiguration loadRabbitMqConfiguration() {
        return getConfiguration(getPathToRabbitMQConfiguration(), RabbitMQConfiguration.class, MAPPER);
    }

    protected MessageRouterConfiguration loadMessageRouterConfiguration() {
        return getConfiguration(getPathToMessageRouterConfiguration(), MessageRouterConfiguration.class, MAPPER);
    }

    protected GrpcRouterConfiguration loadGrpcRouterConfiguration() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(RoutingStrategy.class, new JsonDeserializerRoutingStategy());

        var mapper = new ObjectMapper();
        mapper.registerModule(module);

        return getConfiguration(getPathToGrpcRouterConfiguration(), GrpcRouterConfiguration.class, mapper);
    }

    @Override
    public void close() {
        logger.info("Closing common factory");

        messageRouterParsedBatch.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    logger.error("Failed to close message router for parsed message batches", e);
                }
            }

            return router;
        });

        messageRouterRawBatch.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    logger.error("Failed to close message router for raw message batches", e);
                }
            }

            return router;
        });

        grpcRouter.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    logger.error("Failed to close gRPC router", e);
                }
            }

            return router;
        });

        try {
            prometheusExporter.stop();
        } catch (Exception e) {
            logger.error("Failed to close Prometheus exporter", e);
        }

        logger.info("Common factory has been closed");
    }
}
