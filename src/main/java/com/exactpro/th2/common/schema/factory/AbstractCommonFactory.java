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

package com.exactpro.th2.common.schema.factory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.CommonMetrics;
import com.exactpro.th2.common.metrics.MetricMonitor;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.configuration.ConfigurationManager;
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import com.exactpro.th2.common.schema.exception.CommonFactoryException;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.MessageRouterContext;
import com.exactpro.th2.common.schema.message.MessageRouterMonitor;
import com.exactpro.th2.common.schema.message.NotificationRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext;
import com.exactpro.th2.common.schema.message.impl.monitor.BroadcastMessageRouterMonitor;
import com.exactpro.th2.common.schema.message.impl.monitor.EventMessageRouterMonitor;
import com.exactpro.th2.common.schema.message.impl.monitor.LogMessageRouterMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.MessageConverter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.RabbitCustomRouter;
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_CONSISTENCY_LEVEL;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_TIMEOUT;
import static com.exactpro.th2.common.schema.util.ArchiveUtils.getGzipBase64StringDecoder;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

/**
 * Class for load <b>JSON</b> schema configuration and create {@link GrpcRouter} and {@link MessageRouter}
 *
 * @see CommonFactory
 */
public abstract class AbstractCommonFactory implements AutoCloseable {

    protected static final String DEFAULT_CRADLE_INSTANCE_NAME = "infra";
    protected static final String EXACTPRO_IMPLEMENTATION_VENDOR = "Exactpro Systems LLC";
    /** @deprecated please use {@link #LOG4J_PROPERTIES_DEFAULT_PATH} */
    @Deprecated
    protected static final String LOG4J_PROPERTIES_DEFAULT_PATH_OLD = "/home/etc";
    protected static final String LOG4J_PROPERTIES_DEFAULT_PATH = "/var/th2/config";
    protected static final String LOG4J_PROPERTIES_NAME = "log4j.properties";

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    static  {
        MAPPER.registerModule(new KotlinModule());

        MAPPER.registerModule(new RoutingStrategyModule(MAPPER));
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommonFactory.class);
    private final StringSubstitutor stringSubstitutor;

    private final Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass;
    private final Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass;
    private final Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass;
    private final Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass;
    private final Class<? extends GrpcRouter> grpcRouterClass;
    private final Class<? extends NotificationRouter<EventBatch>> notificationEventBatchRouterClass;
    private final AtomicReference<ConnectionManager> rabbitMqConnectionManager = new AtomicReference<>();
    private final AtomicReference<MessageRouterContext> routerContext = new AtomicReference<>();
    private final AtomicReference<MessageRouter<MessageBatch>> messageRouterParsedBatch = new AtomicReference<>();
    private final AtomicReference<MessageRouter<RawMessageBatch>> messageRouterRawBatch = new AtomicReference<>();
    private final AtomicReference<MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatch = new AtomicReference<>();
    private final AtomicReference<MessageRouter<EventBatch>> eventBatchRouter = new AtomicReference<>();
    private final AtomicReference<NotificationRouter<EventBatch>> notificationEventBatchRouter = new AtomicReference<>();
    private final AtomicReference<String> rootEventId = new AtomicReference<>();
    private final AtomicReference<GrpcRouter> grpcRouter = new AtomicReference<>();
    private final AtomicReference<HTTPServer> prometheusExporter = new AtomicReference<>();
    private final AtomicReference<CradleManager> cradleManager = new AtomicReference<>();
    private final Map<Class<?>, MessageRouter<?>> customMessageRouters = new ConcurrentHashMap<>();
    private final MetricMonitor livenessMonitor = CommonMetrics.registerLiveness("common_factory_liveness");

    static {
        configureLogger();
    }

    /**
     * Create factory with non-default implementations schema classes
     * @param settings {@link FactorySettings}
     */
    public AbstractCommonFactory(FactorySettings settings) {
        messageRouterParsedBatchClass = settings.getMessageRouterParsedBatchClass();
        messageRouterRawBatchClass = settings.getMessageRouterRawBatchClass();
        messageRouterMessageGroupBatchClass = settings.getMessageRouterMessageGroupBatchClass();
        eventBatchRouterClass = settings.getEventBatchRouterClass();
        grpcRouterClass = settings.getGrpcRouterClass();
        notificationEventBatchRouterClass = settings.getNotificationEventBatchRouterClass();
        stringSubstitutor = new StringSubstitutor(key -> defaultIfBlank(settings.getVariables().get(key), System.getenv(key)));
    }

    /**
     * Create factory with default implementation schema classes
     * @deprecated Please use {@link AbstractCommonFactory#AbstractCommonFactory(FactorySettings)}
     */
    @Deprecated(since = "4.0.0", forRemoval = true)
    public AbstractCommonFactory() {
        this(new FactorySettings());
    }

    /**
     * Create factory with non-default implementations schema classes
     * @param messageRouterParsedBatchClass Class for {@link MessageRouter} which work with {@link MessageBatch}
     * @param messageRouterRawBatchClass    Class for {@link MessageRouter} which work with {@link RawMessageBatch}
     * @param eventBatchRouterClass         Class for {@link MessageRouter} which work with {@link EventBatch}
     * @param grpcRouterClass               Class for {@link GrpcRouter}
     * @deprecated Please use {@link AbstractCommonFactory#AbstractCommonFactory(FactorySettings)}
     */
    @Deprecated(since = "4.0.0", forRemoval = true)
    public AbstractCommonFactory(@NotNull Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            @NotNull Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            @NotNull Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
            @NotNull Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            @NotNull Class<? extends GrpcRouter> grpcRouterClass) {
        this(new FactorySettings()
                .messageRouterParsedBatchClass(messageRouterParsedBatchClass)
                .messageRouterRawBatchClass(messageRouterRawBatchClass)
                .messageRouterMessageGroupBatchClass(messageRouterMessageGroupBatchClass)
                .eventBatchRouterClass(eventBatchRouterClass)
                .grpcRouterClass(grpcRouterClass)
        );
    }

    /**
     * Create factory with non-default implementations schema classes
     *
     * @param messageRouterParsedBatchClass Class for {@link MessageRouter} which work with {@link MessageBatch}
     * @param messageRouterRawBatchClass    Class for {@link MessageRouter} which work with {@link RawMessageBatch}
     * @param eventBatchRouterClass         Class for {@link MessageRouter} which work with {@link EventBatch}
     * @param grpcRouterClass               Class for {@link GrpcRouter}
     * @param environmentVariables          map with additional environment variables
     * @deprecated Please use {@link AbstractCommonFactory#AbstractCommonFactory(FactorySettings)}
     */
    @Deprecated(since = "4.0.0", forRemoval = true)
    protected AbstractCommonFactory(@NotNull Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            @NotNull Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            @NotNull Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
            @NotNull Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            @NotNull Class<? extends GrpcRouter> grpcRouterClass,
            @NotNull Map<String, String> environmentVariables) {
        this(new FactorySettings()
                .messageRouterParsedBatchClass(messageRouterParsedBatchClass)
                .messageRouterRawBatchClass(messageRouterRawBatchClass)
                .messageRouterMessageGroupBatchClass(messageRouterMessageGroupBatchClass)
                .eventBatchRouterClass(eventBatchRouterClass)
                .grpcRouterClass(grpcRouterClass)
                .variables(environmentVariables)
        );
    }

    public void start() {
        DefaultExports.initialize();
        PrometheusConfiguration prometheusConfiguration = loadPrometheusConfiguration();

        livenessMonitor.enable();

        this.prometheusExporter.updateAndGet(server -> {
            if (server == null && prometheusConfiguration.getEnabled()) {
                try {
                    return new HTTPServer(prometheusConfiguration.getHost(), prometheusConfiguration.getPort());
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
                    router.init(getMessageRouterContext(), getMessageRouterMessageGroupBatch());
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
                    router.init(getMessageRouterContext(), getMessageRouterMessageGroupBatch());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create raw message router", e);
                }
            }

            return router;
        });
    }

    /**
     * @return Initialized {@link MessageRouter} which works with {@link MessageGroupBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<MessageGroupBatch> getMessageRouterMessageGroupBatch() {
        return messageRouterMessageGroupBatch.updateAndGet(router -> {
            if (router == null) {
                try {
                    router = messageRouterMessageGroupBatchClass.getConstructor().newInstance();
                    router.init(getMessageRouterContext());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create group message router", e);
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
                    router.init(new DefaultMessageRouterContext(
                            getRabbitMqConnectionManager(),
                            MessageRouterMonitor.DEFAULT_MONITOR,
                            getMessageRouterConfiguration(),
                            getBoxConfiguration()
                    ));
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
                    router.init(getGrpcConfiguration(), getGrpcRouterConfiguration());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create GRPC router", e);
                }
            }

            return router;
        });
    }

    /**
     * @return Initialized {@link NotificationRouter} which works with {@link EventBatch}
     * @throws CommonFactoryException if cannot call default constructor from class
     * @throws IllegalStateException  if cannot read configuration
     */
    public NotificationRouter<EventBatch> getNotificationEventBatchRouter() {
        return notificationEventBatchRouter.updateAndGet(router -> {
            if (router == null) {
                try {
                    router = notificationEventBatchRouterClass.getConstructor().newInstance();
                    router.init(getMessageRouterContext());
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                    throw new CommonFactoryException("Can not create notification router", e);
                }
            }
            return router;
        });
    }

    /**
     * Registers custom message router.
     *
     * Unlike the {@link #registerCustomMessageRouter(Class, MessageConverter, Set, Set, String...)} the registered router won't have any additional pins attributes
     * except {@link QueueAttribute#SUBSCRIBE} for subscribe methods and {@link QueueAttribute#PUBLISH} for send methods
     *
     * @see #registerCustomMessageRouter(Class, MessageConverter, Set, Set, String...)
     */
    public <T> void registerCustomMessageRouter(
            Class<T> messageClass,
            MessageConverter<T> messageConverter
    ) {
        registerCustomMessageRouter(messageClass, messageConverter, Collections.emptySet(), Collections.emptySet());
    }

    /**
     * Registers message router for custom type that is passed via {@code messageClass} parameter.<br>
     *
     * @param messageClass custom message class
     * @param messageConverter converter that will be used to convert message to bytes and vice versa
     * @param defaultSendAttributes set of attributes for sending. A pin must have all of them to be selected for sending the message
     * @param defaultSubscribeAttributes set of attributes for subscription. A pin must have all of them to be selected for receiving messages
     * @param <T> custom message type
     * @throws IllegalStateException if the router for {@code messageClass} is already registered
     */
    public <T> void registerCustomMessageRouter(
            Class<T> messageClass,
            MessageConverter<T> messageConverter,
            Set<String> defaultSendAttributes,
            Set<String> defaultSubscribeAttributes,
            String... labels
    ) {
        customMessageRouters.compute(
                messageClass,
                (msgClass, curValue) -> {
                    if (curValue != null) {
                        throw new IllegalStateException("Message router for type " + msgClass.getCanonicalName() + " is already registered");
                    }
                    requireNonNull(labels, "Labels can't be null for custom message router");
                    var router = new RabbitCustomRouter<>(msgClass.getSimpleName(), labels, messageConverter, defaultSendAttributes,
                            defaultSubscribeAttributes);
                    router.init(getRabbitMqConnectionManager(), getMessageRouterConfiguration());
                    return router;
                }
        );
    }

    /**
     * Returns previously registered message router for message of {@code messageClass} type.
     *
     * If the router for that type is not registered yet ,it throws {@link IllegalArgumentException}
     * @param messageClass custom message class
     * @param <T> custom message type
     * @throws IllegalArgumentException if router for specified type is not registered
     * @return the previously registered router for specified type
     */
    @SuppressWarnings("unchecked")
    @NotNull
    public <T> MessageRouter<T> getCustomMessageRouter(Class<T> messageClass) {
        MessageRouter<?> router = customMessageRouters.get(messageClass);
        if (router == null) {
            throw new IllegalArgumentException(
                    "Router for class " + messageClass.getCanonicalName() + "is not registered. Call 'registerCustomMessageRouter' first");
        }
        return (MessageRouter<T>)router;
    }

    /**
     * @return Configuration by specified path
     * @throws IllegalStateException if can not read configuration
     */
    public <T> T getConfiguration(Path configPath, Class<T> configClass, ObjectMapper customObjectMapper) {
        return getConfigurationManager().loadConfiguration(customObjectMapper, stringSubstitutor, configClass, configPath, false);
    }

    /**
     * Load configuration, save and return. If already loaded return saved configuration.
     * @param configClass configuration class
     * @param optional creates an instance of a configuration class via the default constructor if this option is true and the config file doesn't exist or empty
     * @return configuration object
     */
    protected <T> T getConfigurationOrLoad(Class<T> configClass, boolean optional) {
        return getConfigurationManager().getConfigurationOrLoad(MAPPER, stringSubstitutor, configClass, optional);
    }

    public RabbitMQConfiguration getRabbitMqConfiguration() {
        return getConfigurationOrLoad(RabbitMQConfiguration.class, false);
    }

    public ConnectionManagerConfiguration getConnectionManagerConfiguration() {
        return getConfigurationOrLoad(ConnectionManagerConfiguration.class, true);
    }

    public MessageRouterConfiguration getMessageRouterConfiguration() {
        return getConfigurationOrLoad(MessageRouterConfiguration.class, false);
    }

    public GrpcConfiguration getGrpcConfiguration() {
        return getConfigurationManager().getConfigurationOrLoad(MAPPER, stringSubstitutor, GrpcConfiguration.class, false);
    }

    public GrpcRouterConfiguration getGrpcRouterConfiguration() {
        return getConfigurationOrLoad(GrpcRouterConfiguration.class, true);
    }

    public BoxConfiguration getBoxConfiguration() {
        return getConfigurationOrLoad(BoxConfiguration.class, true);
    }

    protected CradleConfidentialConfiguration getCradleConfidentialConfiguration() {
        return getConfigurationOrLoad(CradleConfidentialConfiguration.class, false);
    }

    protected CradleNonConfidentialConfiguration getCradleNonConfidentialConfiguration() {
        return getConfigurationOrLoad(CradleNonConfidentialConfiguration.class, true);
    }

    /**
     * @return Schema cradle configuration
     * @throws IllegalStateException if cannot read configuration
     * @deprecated please use {@link #getCradleManager()}
     */
    @Deprecated
    public CradleConfiguration getCradleConfiguration() {
        return new CradleConfiguration(getCradleConfidentialConfiguration(), getCradleNonConfidentialConfiguration());
    }

    /**
     * @return Cradle manager
     * @throws IllegalStateException if cannot read configuration or initialization failure
     */
    public CradleManager getCradleManager() {
        return cradleManager.updateAndGet(manager -> {
            if (manager == null) {
                try {
                    CradleConfidentialConfiguration confidentialConfiguration = getCradleConfidentialConfiguration();
                    CassandraConnectionSettings cassandraConnectionSettings = new CassandraConnectionSettings(
                            confidentialConfiguration.getHost(),
                            confidentialConfiguration.getPort(),
                            confidentialConfiguration.getDataCenter()
                    );
                    if (StringUtils.isNotEmpty(confidentialConfiguration.getUsername())) {
                        cassandraConnectionSettings.setUsername(confidentialConfiguration.getUsername());
                    }
                    if (StringUtils.isNotEmpty(confidentialConfiguration.getPassword())) {
                        cassandraConnectionSettings.setPassword(confidentialConfiguration.getPassword());
                    }

                    CradleNonConfidentialConfiguration nonConfidentialConfiguration = getCradleNonConfidentialConfiguration();
                    CassandraStorageSettings cassandraStorageSettings = new CassandraStorageSettings(
                            null,
                            nonConfidentialConfiguration.getTimeout() > 0
                                    ? nonConfidentialConfiguration.getTimeout()
                                    : DEFAULT_TIMEOUT,
                            DEFAULT_CONSISTENCY_LEVEL,
                            DEFAULT_CONSISTENCY_LEVEL
                    );
                    if (nonConfidentialConfiguration.getPageSize() > 0) {
                        cassandraStorageSettings.setResultPageSize(nonConfidentialConfiguration.getPageSize());
                    }
                    if (nonConfidentialConfiguration.getCradleMaxMessageBatchSize() > 0) {
                        cassandraStorageSettings.setMaxMessageBatchSize(nonConfidentialConfiguration.getCradleMaxMessageBatchSize());
                    }
                    if (nonConfidentialConfiguration.getCradleMaxEventBatchSize() > 0) {
                        cassandraStorageSettings.setMaxTestEventBatchSize(nonConfidentialConfiguration.getCradleMaxEventBatchSize());
                    }

                    manager = new CassandraCradleManager(
                            cassandraConnectionSettings,
                            cassandraStorageSettings,
                            nonConfidentialConfiguration.getPrepareStorage()
                    );
                } catch (CradleStorageException | RuntimeException | IOException e) {
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
     * @return Java bean with custom configuration, or <b>NULL</b> if configuration does not exists and cannot call default constructor from java bean class
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
            List<Path> dictionaries = null;
            Path dictionaryTypeDictionary = dictionaryType.getDictionary(getPathToDictionariesDir());
            if (Files.exists(dictionaryTypeDictionary) && Files.isDirectory(dictionaryTypeDictionary)) {
                dictionaries = Files.list(dictionaryType.getDictionary(getPathToDictionariesDir()))
                        .filter(Files::isRegularFile)
                        .collect(Collectors.toList());
            }

            // Find with old format
            if (dictionaries == null || dictionaries.isEmpty()) {
                dictionaries = Files.list(getOldPathToDictionariesDir())
                        .filter(path -> Files.isRegularFile(path) && path.getFileName().toString().contains(dictionaryType.name()))
                        .collect(Collectors.toList());
            }

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
     * If root event does not exist, it creates root event with its name = box name and timestamp
     * @return root event id
     */
    @Nullable
    public String getRootEventId() {
        return rootEventId.updateAndGet(id -> {
            if (id == null) {
                try {
                    String boxName = getBoxConfiguration().getBoxName();
                    if (boxName == null) {
                        return null;
                    }

                    com.exactpro.th2.common.grpc.Event rootEvent = Event.start().endTimestamp()
                            .name(boxName + " " + Instant.now())
                            .description("Root event")
                            .status(Event.Status.PASSED)
                            .type("Microservice")
                            .toProtoEvent(null);

                    try {
                        getEventBatchRouter().sendAll(EventBatch.newBuilder().addEvents(rootEvent).build());
                        return rootEvent.getId().getId();
                    } catch (IOException e) {
                        throw new CommonFactoryException("Can not send root event", e);
                    }
                } catch (JsonProcessingException e) {
                    throw new CommonFactoryException("Can not create root event", e);
                }
            }
            return id;
        });
    }

    protected abstract ConfigurationManager getConfigurationManager();

    /**
     * @return Path to custom configuration
     */
    protected abstract Path getPathToCustomConfiguration();

    /**
     * @return Path to dictionary
     */
    protected abstract Path getPathToDictionariesDir();

    protected abstract Path getOldPathToDictionariesDir();

    /**
     * @return Context for all routers except event router
     */
    protected MessageRouterContext getMessageRouterContext() {
        return routerContext.updateAndGet(ctx -> {
           if (ctx == null) {
               try {

                   MessageRouterMonitor contextMonitor;
                   String rootEventId = getRootEventId();
                   if (rootEventId == null) {
                       contextMonitor = new LogMessageRouterMonitor();
                   } else {
                       contextMonitor = new BroadcastMessageRouterMonitor(new LogMessageRouterMonitor(), new EventMessageRouterMonitor(getEventBatchRouter(), rootEventId));
                   }

                   return new DefaultMessageRouterContext(
                           getRabbitMqConnectionManager(),
                           contextMonitor,
                           getMessageRouterConfiguration(),
                           getBoxConfiguration()
                   );
               } catch (Exception e) {
                   throw new CommonFactoryException("Can not create message router context", e);
               }
           }
           return ctx;
        });
    }

    protected PrometheusConfiguration loadPrometheusConfiguration() {
        return getConfigurationOrLoad(PrometheusConfiguration.class, true);
    }

    protected ConnectionManager createRabbitMQConnectionManager() {
        return new ConnectionManager(getRabbitMqConfiguration(), getConnectionManagerConfiguration(), livenessMonitor::disable);
    }

    protected ConnectionManager getRabbitMqConnectionManager() {
        return rabbitMqConnectionManager.updateAndGet(connectionManager -> {
            if (connectionManager == null) {
                return createRabbitMQConnectionManager();
            }
            return connectionManager;
        });
    }

    public MessageID.Builder newMessageIDBuilder() {
        return MessageID.newBuilder()
                .setBookName(getBoxConfiguration().getBookName());
    }

    public EventID.Builder newEventIDBuilder() {
        return EventID.newBuilder()
                .setBookName(getBoxConfiguration().getBookName());
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

        messageRouterMessageGroupBatch.getAndUpdate(router -> {
            if (router != null) {
                try {
                    router.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close message router for message group batches", e);
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

        customMessageRouters.forEach((messageType, router) -> {
            try {
                router.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close custom router for {}", messageType, e);
            }
        });

        cradleManager.getAndUpdate(manager -> {
            if (manager != null) {
                try {
                    manager.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close Cradle manager", e);
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

    protected static void configureLogger(String... paths) {
        List<String> listPath = new ArrayList<>();
        listPath.add(LOG4J_PROPERTIES_DEFAULT_PATH);
        listPath.add(LOG4J_PROPERTIES_DEFAULT_PATH_OLD);
        listPath.addAll(Arrays.asList(requireNonNull(paths, "Paths can't be null")));
        listPath.stream()
                .map(path -> Path.of(path, LOG4J_PROPERTIES_NAME))
                .filter(Files::exists)
                .findFirst()
                .ifPresentOrElse(path -> {
                            LogManager.resetConfiguration();
                            PropertyConfigurator.configure(path.toString());
                            LOGGER.info("Logger configuration from {} file is applied", path);
                        },
                        () -> LOGGER.info("Neither of {} paths contains {} file. Use default configuration", listPath, LOG4J_PROPERTIES_NAME));
        loggingManifests();
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
                    .forEach(attributes -> LOGGER.info("Manifest title {}, version {}"
                            , attributes.getValue(Name.IMPLEMENTATION_TITLE), attributes.getValue(Name.IMPLEMENTATION_VERSION)));
        } catch (IOException e) {
            LOGGER.warn("Manifest searching failure", e);
        }
    }
}
