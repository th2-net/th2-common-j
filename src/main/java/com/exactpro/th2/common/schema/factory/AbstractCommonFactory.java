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

package com.exactpro.th2.common.schema.factory;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_MAX_EVENT_BATCH_SIZE;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_MAX_MESSAGE_BATCH_SIZE;
import static com.exactpro.th2.common.schema.util.ArchiveUtils.getGzipBase64StringDecoder;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookupFactory;
import org.apache.log4j.PropertyConfigurator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import com.exactpro.th2.common.schema.event.EventBatchRouter;
import com.exactpro.th2.common.schema.exception.CommonFactoryException;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter;
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy;
import com.exactpro.th2.common.schema.strategy.route.json.JsonDeserializerRoutingStategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * Class for load <b>JSON</b> schema configuration and create {@link GrpcRouter} and {@link MessageRouter}
 *
 * @see CommonFactory
 */
public abstract class AbstractCommonFactory implements AutoCloseable {

    protected static final String DEFAULT_CRADLE_INSTANCE_NAME = "infra";
    protected static final String EXACTPRO_IMPLEMENTATION_VENDOR = "Exactpro Systems LLC";
    protected static final String LOG4J_PROPERTIES_DEFAULT_PATH = "/home/etc/log4j.properties";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommonFactory.class);
    private final AtomicReference<RabbitMQConfiguration> rabbitMqConfiguration = new AtomicReference<>();
    private final AtomicReference<MessageRouterConfiguration> messageRouterConfiguration = new AtomicReference<>();
    private final AtomicReference<GrpcRouterConfiguration> grpcRouterConfiguration = new AtomicReference<>();

    public RabbitMQConfiguration getRabbitMqConfiguration() {
        return rabbitMqConfiguration.updateAndGet(this::loadRabbitMqConfiguration);
    }

    public MessageRouterConfiguration getMessageRouterConfiguration() {
        return messageRouterConfiguration.updateAndGet(this::loadMessageRouterConfiguration);
    }

    public GrpcRouterConfiguration getGrpcRouterConfiguration() {
        return grpcRouterConfiguration.updateAndGet(this::loadGrpcRouterConfiguration);
    }

    private final Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass;
    private final Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass;
    private final Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass;
    private final Class<? extends GrpcRouter> grpcRouterClass;
    private final AtomicReference<ConnectionManager> rabbitMqConnectionManager = new AtomicReference<>();
    private final AtomicReference<MessageRouter<MessageBatch>> messageRouterParsedBatch = new AtomicReference<>();
    private final AtomicReference<MessageRouter<RawMessageBatch>> messageRouterRawBatch = new AtomicReference<>();
    private final AtomicReference<MessageRouter<EventBatch>> eventBatchRouter = new AtomicReference<>();
    private final AtomicReference<GrpcRouter> grpcRouter = new AtomicReference<>();
    private final AtomicReference<HTTPServer> prometheusExporter = new AtomicReference<>();
    private final AtomicReference<CradleManager> cradleManager = new AtomicReference<>();

    static {
        PropertyConfigurator.configure(LOG4J_PROPERTIES_DEFAULT_PATH);
        loggingManifests();
    }

    /**
     * Create factory with default implementation schema classes
     */
    public AbstractCommonFactory() {
        this(RabbitParsedBatchRouter.class, RabbitRawBatchRouter.class, EventBatchRouter.class, DefaultGrpcRouter.class);
    }

    /**
     * Create factory with not-default implementations schema classes
     *
     * @param messageRouterParsedBatchClass Class for {@link MessageRouter} which work with {@link MessageBatch}
     * @param messageRouterRawBatchClass    Class for {@link MessageRouter} which work with {@link RawMessageBatch}
     * @param eventBatchRouterClass         Class for {@link MessageRouter} which work with {@link EventBatch}
     * @param grpcRouterClass               Class for {@link GrpcRouter}
     */
    public AbstractCommonFactory(@NotNull Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                                 @NotNull Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                                 @NotNull Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                                 @NotNull Class<? extends GrpcRouter> grpcRouterClass) {
        this.messageRouterParsedBatchClass = messageRouterParsedBatchClass;
        this.messageRouterRawBatchClass = messageRouterRawBatchClass;
        this.eventBatchRouterClass = eventBatchRouterClass;
        this.grpcRouterClass = grpcRouterClass;
    }
    public void start() {
        DefaultExports.initialize();
        PrometheusConfiguration prometheusConfiguration = loadPrometheusConfiguration();

        CommonMetrics.setLiveness(true);

        this.prometheusExporter.updateAndGet(server -> {
            if (server == null && prometheusConfiguration.getEnabled()) {
                try {
                    server = new HTTPServer(prometheusConfiguration.getHost(), prometheusConfiguration.getPort());
                } catch (IOException e) {
                    throw new CommonFactoryException("Failed to create Prometheus exporter", e);
                }
            }
            return server;
        });
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
                    router.init(getRabbitMqConnectionManager(), getMessageRouterConfiguration());
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
                    router.init(getRabbitMqConnectionManager(), getMessageRouterConfiguration());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create raw message router", e);
                }
            }

            return router;
        });
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link EventBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<EventBatch> getEventBatchRouter() {
        return eventBatchRouter.updateAndGet(router -> {
            if (router == null) {
                try {
                    router = eventBatchRouterClass.getConstructor().newInstance();
                    router.init(getRabbitMqConnectionManager(), getMessageRouterConfiguration());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create event batch router", e);
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
            String sourceContent = new String(Files.readAllBytes(configPath));
            LOGGER.info("Configuration path {} source content {}", configPath, sourceContent);

            StringSubstitutor stringSubstitutor = new StringSubstitutor(StringLookupFactory.INSTANCE.environmentVariableStringLookup());
            String content = stringSubstitutor.replace(sourceContent);
            return customObjectMapper.readerFor(configClass).readValue(content);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Cannot read %s configuration", configClass.getName()), e);
        }
    }

    /**
     * @return Schema cradle configuration
     * @throws IllegalStateException if cannot read configuration
     * @deprecated please use {@link #getCradleManager()}
     */
    @Deprecated
    public CradleConfiguration getCradleConfiguration() {
        return getConfiguration(getPathToCradleConfiguration(), CradleConfiguration.class, MAPPER);
    }

    /**
     * @return Cradle manager
     * @throws IllegalStateException if cannot read configuration or initialization failure
     */
    public CradleManager getCradleManager() {
        return cradleManager.updateAndGet(manager -> {
            if (manager == null) {
                try {
                    CradleConfiguration cradleConfiguration = getCradleConfiguration();
                    CassandraConnectionSettings cassandraConnectionSettings = new CassandraConnectionSettings(
                            cradleConfiguration.getDataCenter(),
                            cradleConfiguration.getHost(),
                            cradleConfiguration.getPort(),
                            cradleConfiguration.getKeyspace());

                    if (StringUtils.isNotEmpty(cradleConfiguration.getUsername())) {
                        cassandraConnectionSettings.setUsername(cradleConfiguration.getUsername());
                    }

                    if (StringUtils.isNotEmpty(cradleConfiguration.getPassword())) {
                        cassandraConnectionSettings.setPassword(cradleConfiguration.getPassword());
                    }

                    manager = new CassandraCradleManager(new CassandraConnection(cassandraConnectionSettings));
                    manager.init(defaultIfBlank(cradleConfiguration.getCradleInstanceName(), DEFAULT_CRADLE_INSTANCE_NAME), false,
                            cradleConfiguration.getCradleMaxMessageBatchSize() > 0 ? cradleConfiguration.getCradleMaxMessageBatchSize() : DEFAULT_MAX_MESSAGE_BATCH_SIZE,
                            cradleConfiguration.getCradleMaxEventBatchSize() > 0 ? cradleConfiguration.getCradleMaxEventBatchSize() : DEFAULT_MAX_EVENT_BATCH_SIZE);
                } catch (CradleStorageException | RuntimeException e) {
                    throw new CommonFactoryException("Cannot create Cradle manager", e);
                }
            }

            return manager;
        });

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

    /**
     * @return Path to configuration for prometheus server
     * @see PrometheusConfiguration
     */
    protected abstract Path getPathToPrometheusConfiguration();

    protected RabbitMQConfiguration loadRabbitMqConfiguration(RabbitMQConfiguration currentValue) {
        return currentValue == null ? getConfiguration(getPathToRabbitMQConfiguration(), RabbitMQConfiguration.class, MAPPER) : currentValue;

    }

    protected MessageRouterConfiguration loadMessageRouterConfiguration(MessageRouterConfiguration currentValue) {
        return currentValue == null ? getConfiguration(getPathToMessageRouterConfiguration(), MessageRouterConfiguration.class, MAPPER) : currentValue;
    }

    protected GrpcRouterConfiguration loadGrpcRouterConfiguration(GrpcRouterConfiguration currentValue) {
        if (currentValue == null) {
            SimpleModule module = new SimpleModule();
            module.addDeserializer(RoutingStrategy.class, new JsonDeserializerRoutingStategy());

            var mapper = new ObjectMapper();
            mapper.registerModule(module);

            return getConfiguration(getPathToGrpcRouterConfiguration(), GrpcRouterConfiguration.class, mapper);
        }
        return currentValue;
    }

    protected PrometheusConfiguration loadPrometheusConfiguration() {
        try {
            return getConfiguration(getPathToPrometheusConfiguration(), PrometheusConfiguration.class, MAPPER);
        } catch (IllegalStateException e) {
            LOGGER.warn("Cannot load prometheus configuration from file by path = '{}'. Use default configuration", getPathToPrometheusConfiguration(), e);
            return new PrometheusConfiguration();
        }
    }

    protected ConnectionManager createRabbitMQConnectionManager() {
        return new ConnectionManager(getRabbitMqConfiguration(), () -> CommonMetrics.setLiveness(false));
    }

    protected ConnectionManager getRabbitMqConnectionManager() {
        return rabbitMqConnectionManager.updateAndGet(connectionManager -> {
            if (connectionManager == null) {
                return createRabbitMQConnectionManager();
            }
            return connectionManager;
        });
    }

    @Override
    public void close() {
        LOGGER.info("Closing common factory");

        messageRouterParsedBatch.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close message router for parsed message batches", e);
                }
            }

            return router;
        });

        messageRouterRawBatch.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close message router for raw message batches", e);
                }
            }

            return router;
        });

        rabbitMqConnectionManager.updateAndGet(connection -> {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close RabbitMQ connection", e);
                }
            }
            return connection;
        });

        grpcRouter.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close gRPC router", e);
                }
            }

            return router;
        });

        cradleManager.getAndUpdate(manager -> {
            if (manager != null) {
                try {
                    manager.dispose();
                } catch (Exception e) {
                    LOGGER.error("Failed to dispose Cradle manager", e);
                }
            }

            return manager;
        });

        prometheusExporter.updateAndGet(server -> {
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception e) {
                    LOGGER.error("Failed to close Prometheus exporter", e);
                }
            }
            return null;
        });

        LOGGER.info("Common factory has been closed");
    }

    private static void loggingManifests() {
        try {
            Iterator<URL> urlIterator = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME).asIterator();
            StreamSupport.stream(Spliterators.spliteratorUnknownSize(urlIterator, 0), false)
                    .map(url -> {
                        try (InputStream inputStream = url.openStream()) {
                            return new Manifest(inputStream);
                        } catch (IOException e) {
                            LOGGER.warn("Manifest '{}' loading failere", url, e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .map(Manifest::getMainAttributes)
                    .filter(attributes -> EXACTPRO_IMPLEMENTATION_VENDOR.equals(attributes.getValue(Name.IMPLEMENTATION_VENDOR)))
                    .forEach(attributes -> {
                        LOGGER.info("Manifest title {}, version {}"
                                , attributes.getValue(Name.IMPLEMENTATION_TITLE), attributes.getValue(Name.IMPLEMENTATION_VERSION));
                    });
        } catch (IOException e) {
            LOGGER.warn("Manifest searching failure", e);
        }
    }
}
