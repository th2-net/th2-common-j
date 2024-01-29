/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.TransportGroupBatchRouter;
import com.exactpro.th2.common.schema.strategy.route.json.RoutingStrategyModule;
import com.exactpro.th2.common.schema.util.Log4jConfigUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.kotlin.KotlinFeature;
import com.fasterxml.jackson.module.kotlin.KotlinModule;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;
import static com.exactpro.cradle.CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_COUNTER_PERSISTENCE_INTERVAL_MS;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE;
import static com.exactpro.cradle.cassandra.CassandraStorageSettings.DEFAULT_RESULT_PAGE_SIZE;
import static com.exactpro.th2.common.schema.factory.LazyProvider.lazy;
import static com.exactpro.th2.common.schema.factory.LazyProvider.lazyAutocloseable;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

/**
 * Class for load <b>JSON</b> schema configuration and create {@link GrpcRouter} and {@link MessageRouter}
 *
 * @see CommonFactory
 */
public abstract class AbstractCommonFactory implements AutoCloseable {

    /**
     * @deprecated please use {@link #LOG4J_PROPERTIES_DEFAULT_PATH}
     */
    @Deprecated
    protected static final Path LOG4J_PROPERTIES_DEFAULT_PATH_OLD = Path.of("/home/etc");
    protected static final Path LOG4J_PROPERTIES_DEFAULT_PATH = Path.of("/var/th2/config");
    protected static final String LOG4J2_PROPERTIES_NAME = "log4j2.properties";

    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModules(
                new KotlinModule.Builder()
                        .withReflectionCacheSize(512)
                        .configure(KotlinFeature.NullToEmptyCollection, false)
                        .configure(KotlinFeature.NullToEmptyMap, false)
                        .configure(KotlinFeature.NullIsSameAsDefault, false)
                        .configure(KotlinFeature.SingletonSupport, false)
                        .configure(KotlinFeature.StrictNullChecks, false)
                        .build(),
                new RoutingStrategyModule(MAPPER),
                new JavaTimeModule()
        );
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCommonFactory.class);
    private final StringSubstitutor stringSubstitutor;

    private final Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass;
    private final Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass;
    private final Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass;
    private final Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass;
    private final Class<? extends GrpcRouter> grpcRouterClass;
    private final Class<? extends NotificationRouter<EventBatch>> notificationEventBatchRouterClass;
    private final LazyProvider<ConnectionManager> rabbitMqConnectionManager =
            lazyAutocloseable("connection-manager", this::createRabbitMQConnectionManager);
    private final LazyProvider<MessageRouterContext> routerContext =
            lazy("router-context", this::createMessageRouterContext);
    private final LazyProvider<MessageRouter<MessageBatch>> messageRouterParsedBatch =
            lazyAutocloseable("parsed-message-router", this::createMessageRouterParsedBatch);
    private final LazyProvider<MessageRouter<RawMessageBatch>> messageRouterRawBatch =
            lazyAutocloseable("raw-message-router", this::createMessageRouterRawBatch);
    private final LazyProvider<MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatch =
            lazyAutocloseable("group-message-router", this::createMessageRouterGroupBatch);
    private final LazyProvider<MessageRouter<GroupBatch>> transportGroupBatchRouter =
            lazyAutocloseable("transport-router", this::createTransportGroupBatchMessageRouter);
    private final LazyProvider<MessageRouter<EventBatch>> eventBatchRouter =
            lazyAutocloseable("event-router", this::createEventBatchRouter);
    private final LazyProvider<NotificationRouter<EventBatch>> notificationEventBatchRouter =
            lazyAutocloseable("notification-router", this::createNotificationEventBatchRouter);
    private final LazyProvider<EventID> rootEventId = lazy("root-event-id", this::createRootEventID);
    private final LazyProvider<GrpcRouter> grpcRouter =
            lazyAutocloseable("grpc-router", this::createGrpcRouter);
    private final LazyProvider<HTTPServer> prometheusExporter =
            lazyAutocloseable("prometheus-exporter", this::createPrometheusHTTPServer);

    private final LazyProvider<CradleManager> cradleManager =
            lazyAutocloseable("cradle-manager", this::createCradleManager);

    private final Map<Class<?>, MessageRouter<?>> customMessageRouters = new ConcurrentHashMap<>();
    private final MetricMonitor livenessMonitor = CommonMetrics.registerLiveness("common_factory_liveness");
    static {
        configureLogger();
    }

    /**
     * Create factory with non-default implementations schema classes
     *
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

    public void start() {
        DefaultExports.initialize();

        // init exporter
        prometheusExporter.getOrNull();
        livenessMonitor.enable();
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link MessageBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<MessageBatch> getMessageRouterParsedBatch() {
        return messageRouterParsedBatch.get();
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link RawMessageBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<RawMessageBatch> getMessageRouterRawBatch() {
        return messageRouterRawBatch.get();
    }

    /**
     * @return Initialized {@link MessageRouter} which works with {@link GroupBatch}
     * @throws IllegalStateException if can not read configuration
     */
    public MessageRouter<GroupBatch> getTransportGroupBatchRouter() {
        return transportGroupBatchRouter.get();
    }

    /**
     * @return Initialized {@link MessageRouter} which works with {@link MessageGroupBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<MessageGroupBatch> getMessageRouterMessageGroupBatch() {
        return messageRouterMessageGroupBatch.get();
    }

    /**
     * @return Initialized {@link MessageRouter} which work with {@link EventBatch}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public MessageRouter<EventBatch> getEventBatchRouter() {
        return eventBatchRouter.get();
    }

    /**
     * @return Initialized {@link GrpcRouter}
     * @throws CommonFactoryException if can not call default constructor from class
     * @throws IllegalStateException  if can not read configuration
     */
    public GrpcRouter getGrpcRouter() {
        return grpcRouter.get();
    }

    /**
     * @return Initialized {@link NotificationRouter} which works with {@link EventBatch}
     * @throws CommonFactoryException if cannot call default constructor from class
     * @throws IllegalStateException  if cannot read configuration
     */
    public NotificationRouter<EventBatch> getNotificationEventBatchRouter() {
        return notificationEventBatchRouter.get();
    }

    /**
     * Registers custom message router.
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
     * @param messageClass               custom message class
     * @param messageConverter           converter that will be used to convert message to bytes and vice versa
     * @param defaultSendAttributes      set of attributes for sending. A pin must have all of them to be selected for sending the message
     * @param defaultSubscribeAttributes set of attributes for subscription. A pin must have all of them to be selected for receiving messages
     * @param <T>                        custom message type
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
                    MessageRouter<T> router = new RabbitCustomRouter<>(msgClass.getSimpleName(), labels, messageConverter, defaultSendAttributes,
                            defaultSubscribeAttributes);
                    router.init(getMessageRouterContext());
                    return router;
                }
        );
    }

    /**
     * Returns previously registered message router for message of {@code messageClass} type.
     * If the router for that type is not registered yet ,it throws {@link IllegalArgumentException}
     *
     * @param messageClass custom message class
     * @param <T>          custom message type
     * @return the previously registered router for specified type
     * @throws IllegalArgumentException if router for specified type is not registered
     */
    @SuppressWarnings("unchecked")
    @NotNull
    public <T> MessageRouter<T> getCustomMessageRouter(Class<T> messageClass) {
        MessageRouter<?> router = customMessageRouters.get(messageClass);
        if (router == null) {
            throw new IllegalArgumentException(
                    "Router for class " + messageClass.getCanonicalName() + "is not registered. Call 'registerCustomMessageRouter' first");
        }
        return (MessageRouter<T>) router;
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
     *
     * @param configClass configuration class
     * @param optional    creates an instance of a configuration class via the default constructor if this option is true and the config file doesn't exist or empty
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

    /**
     * @return Cradle manager
     * @throws CommonFactoryException if cradle manager was not initialized
     */
    public CradleManager getCradleManager() {
        return cradleManager.get();

    }

    @Nullable
    private HTTPServer createPrometheusHTTPServer() {
        PrometheusConfiguration configuration = loadPrometheusConfiguration();
        if (configuration.getEnabled()) {
            try {
                return new HTTPServer(configuration.getHost(), configuration.getPort());
            } catch (IOException e) {
                throw new CommonFactoryException("Failed to create Prometheus exporter", e);
            }
        }
        return null;
    }

    private MessageRouter<MessageBatch> createMessageRouterParsedBatch() {
        try {
            MessageRouter<MessageBatch> router = messageRouterParsedBatchClass.getConstructor().newInstance();
            router.init(getMessageRouterContext(), getMessageRouterMessageGroupBatch());
            return router;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new CommonFactoryException("Can not create parsed message router", e);
        }
    }

    private MessageRouter<RawMessageBatch> createMessageRouterRawBatch() throws NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        MessageRouter<RawMessageBatch> router = messageRouterRawBatchClass.getConstructor().newInstance();
        router.init(getMessageRouterContext(), getMessageRouterMessageGroupBatch());
        return router;
    }

    private MessageRouter<GroupBatch> createTransportGroupBatchMessageRouter() {
        var router = new TransportGroupBatchRouter();
        router.init(getMessageRouterContext());
        return router;
    }

    private MessageRouter<MessageGroupBatch> createMessageRouterGroupBatch() throws NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        var router = messageRouterMessageGroupBatchClass.getConstructor().newInstance();
        router.init(getMessageRouterContext());
        return router;
    }

    private MessageRouter<EventBatch> createEventBatchRouter() throws NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        var router = eventBatchRouterClass.getConstructor().newInstance();
        router.init(createEventRouterContext());
        return router;
    }

    @NotNull
    private MessageRouterContext createEventRouterContext() {
        return createRouterContext(MessageRouterMonitor.DEFAULT_MONITOR);
    }

    private GrpcRouter createGrpcRouter() throws NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        GrpcRouter router = grpcRouterClass.getConstructor().newInstance();
        router.init(getGrpcConfiguration(), getGrpcRouterConfiguration());
        return router;
    }

    private NotificationRouter<EventBatch> createNotificationEventBatchRouter() throws NoSuchMethodException,
            InvocationTargetException, InstantiationException, IllegalAccessException {
        var router = notificationEventBatchRouterClass.getConstructor().newInstance();
        router.init(getMessageRouterContext());
        return router;
    }

    private CradleConfidentialConfiguration getCradleConfidentialConfiguration() {
        return getConfigurationOrLoad(CradleConfidentialConfiguration.class, false);
    }

    private CradleNonConfidentialConfiguration getCradleNonConfidentialConfiguration() {
        return getConfigurationOrLoad(CradleNonConfidentialConfiguration.class, true);
    }

    private CassandraStorageSettings getCassandraStorageSettings() {
        return getConfigurationOrLoad(CassandraStorageSettings.class, true);
    }

    private CradleManager createCradleManager() {
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

            // Deserialize on config by two different beans for backward compatibility
            CradleNonConfidentialConfiguration nonConfidentialConfiguration = getCradleNonConfidentialConfiguration();
            // FIXME: this approach should be replaced to module structure in future
            CassandraStorageSettings cassandraStorageSettings = getCassandraStorageSettings();
            cassandraStorageSettings.setKeyspace(confidentialConfiguration.getKeyspace());

            if (cassandraStorageSettings.getResultPageSize() == DEFAULT_RESULT_PAGE_SIZE && nonConfidentialConfiguration.getPageSize() > 0) {
                cassandraStorageSettings.setResultPageSize(nonConfidentialConfiguration.getPageSize());
            }
            if (cassandraStorageSettings.getMaxMessageBatchSize() == DEFAULT_MAX_MESSAGE_BATCH_SIZE && nonConfidentialConfiguration.getCradleMaxMessageBatchSize() > 0) {
                cassandraStorageSettings.setMaxMessageBatchSize((int) nonConfidentialConfiguration.getCradleMaxMessageBatchSize());
            }
            if (cassandraStorageSettings.getMaxTestEventBatchSize() == DEFAULT_MAX_TEST_EVENT_BATCH_SIZE && nonConfidentialConfiguration.getCradleMaxEventBatchSize() > 0) {
                cassandraStorageSettings.setMaxTestEventBatchSize((int) nonConfidentialConfiguration.getCradleMaxEventBatchSize());
            }
            if (cassandraStorageSettings.getCounterPersistenceInterval() == DEFAULT_COUNTER_PERSISTENCE_INTERVAL_MS && nonConfidentialConfiguration.getStatisticsPersistenceIntervalMillis() >= 0) {
                cassandraStorageSettings.setCounterPersistenceInterval((int) nonConfidentialConfiguration.getStatisticsPersistenceIntervalMillis());
            }
            if (cassandraStorageSettings.getMaxUncompressedTestEventSize() == DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE && nonConfidentialConfiguration.getMaxUncompressedEventBatchSize() > 0) {
                cassandraStorageSettings.setMaxUncompressedTestEventSize((int) nonConfidentialConfiguration.getMaxUncompressedEventBatchSize());
            }

            return new CassandraCradleManager(
                    cassandraConnectionSettings,
                    cassandraStorageSettings,
                    nonConfidentialConfiguration.getPrepareStorage()
            );
        } catch (CradleStorageException | RuntimeException | IOException e) {
            throw new CommonFactoryException("Cannot create Cradle manager", e);
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
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                     NoSuchMethodException e) {
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
     * Read first and only one dictionary
     *
     * @return Dictionary as {@link InputStream}
     * @throws IllegalStateException if can not read dictionary or found more than one target
     */
    public abstract InputStream loadSingleDictionary();

    /**
     * @return list of available dictionary aliases or an empty list
     * @throws IllegalStateException if can not read dictionary
     */
    public abstract Set<String> getDictionaryAliases();

    /**
     * @param alias name of dictionary
     * @return Dictionary as {@link InputStream}
     * @throws IllegalStateException if can not read dictionary
     */
    public abstract InputStream loadDictionary(String alias);

    /**
     * Read dictionary of {@link DictionaryType#MAIN} type
     *
     * @return Dictionary as {@link InputStream}
     * @throws IllegalStateException if can not read dictionary
     */
    @Deprecated(since = "3.33.0", forRemoval = true)
    public abstract InputStream readDictionary();

    /**
     * @param dictionaryType desired type of dictionary
     * @return Dictionary as {@link InputStream}
     * @throws IllegalStateException if can not read dictionary
     * @deprecated Dictionary types will be removed in future releases of infra, use alias instead
     */
    @Deprecated(since = "3.33.0", forRemoval = true)
    public abstract InputStream readDictionary(DictionaryType dictionaryType);

    /**
     * If root event does not exist, it creates root event with its book name = box book name and name = box name and timestamp
     *
     * @return root event id
     */
    @NotNull
    public EventID getRootEventId() {
        return rootEventId.get();
    }

    @NotNull
    private EventID createRootEventID() throws IOException {
        BoxConfiguration boxConfiguration = getBoxConfiguration();
        com.exactpro.th2.common.grpc.Event rootEvent = Event
                .start()
                .endTimestamp()
                .name(boxConfiguration.getBoxName() + " " + Instant.now())
                .description("Root event")
                .status(Event.Status.PASSED)
                .type("Microservice")
                .toProto(boxConfiguration.getBookName(), boxConfiguration.getBoxName());

        try {
            getEventBatchRouter().sendAll(EventBatch.newBuilder().addEvents(rootEvent).build());
            return rootEvent.getId();
        } catch (IOException e) {
            throw new CommonFactoryException("Can not send root event", e);
        }
    }

    protected abstract ConfigurationManager getConfigurationManager();

    /**
     * @return Path to custom configuration
     */
    protected abstract Path getPathToCustomConfiguration();

    /**
     * @return Path to dictionaries with type dir
     */
    @Deprecated(since = "3.33.0", forRemoval = true)
    protected abstract Path getPathToDictionaryTypesDir();

    /**
     * @return Path to dictionaries with alias dir
     */
    protected abstract Path getPathToDictionaryAliasesDir();

    @Deprecated(since = "3.33.0", forRemoval = true)
    protected abstract Path getOldPathToDictionariesDir();

    /**
     * @return Context for all routers except event router
     */
    protected MessageRouterContext getMessageRouterContext() {
        return routerContext.get();
    }

    @NotNull
    private MessageRouterContext createMessageRouterContext() {
        MessageRouterMonitor contextMonitor = new BroadcastMessageRouterMonitor(
                new LogMessageRouterMonitor(),
                new EventMessageRouterMonitor(
                        getEventBatchRouter(),
                        getRootEventId()
                )
        );

        return createRouterContext(contextMonitor);
    }

    @NotNull
    private MessageRouterContext createRouterContext(MessageRouterMonitor contextMonitor) {
        return new DefaultMessageRouterContext(
                getRabbitMqConnectionManager(),
                contextMonitor,
                getMessageRouterConfiguration(),
                getBoxConfiguration()
        );
    }

    protected PrometheusConfiguration loadPrometheusConfiguration() {
        return getConfigurationOrLoad(PrometheusConfiguration.class, true);
    }

    protected ConnectionManager createRabbitMQConnectionManager() {
        return new ConnectionManager(getRabbitMqConfiguration(), getConnectionManagerConfiguration(), livenessMonitor::disable);
    }

    protected ConnectionManager getRabbitMqConnectionManager() {
        return rabbitMqConnectionManager.get();
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

        try {
            messageRouterParsedBatch.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close message router for parsed message batches", e);
        }

        try {
            messageRouterRawBatch.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close message router for raw message batches", e);
        }

        try {
            messageRouterMessageGroupBatch.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close message router for message group batches", e);
        }

        try {
            rabbitMqConnectionManager.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close RabbitMQ connection", e);
        }

        try {
            grpcRouter.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close gRPC router", e);
        }

        customMessageRouters.forEach((messageType, router) -> {
            try {
                router.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close custom router for {}", messageType, e);
            }
        });

        try {
            cradleManager.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close Cradle manager", e);
        }

        try {
            prometheusExporter.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close Prometheus exporter", e);
        }

        LOGGER.info("Common factory has been closed");
    }

    protected static void configureLogger(Path... paths) {
        List<Path> listPath = new ArrayList<>();
        listPath.add(LOG4J_PROPERTIES_DEFAULT_PATH);
        listPath.add(LOG4J_PROPERTIES_DEFAULT_PATH_OLD);
        listPath.addAll(Arrays.asList(requireNonNull(paths, "Paths can't be null")));
        Log4jConfigUtils log4jConfigUtils = new Log4jConfigUtils();
        log4jConfigUtils.configure(listPath, LOG4J2_PROPERTIES_NAME);
        ExactproMetaInf.logging();
    }
}
