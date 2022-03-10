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

package com.exactpro.th2.common.schema.factory;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.configuration.ConfigurationManager;
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import com.exactpro.th2.common.schema.event.EventBatchRouter;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import kotlin.text.Charsets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exactpro.th2.common.schema.util.ArchiveUtils.getGzipBase64StringDecoder;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Default implementation for {@link AbstractCommonFactory}
 */
public class CommonFactory extends AbstractCommonFactory {

    private static final Path CONFIG_DEFAULT_PATH = Path.of("/var/th2/config/");

    private static final String RABBIT_MQ_FILE_NAME = "rabbitMQ.json";
    private static final String ROUTER_MQ_FILE_NAME = "mq.json";
    private static final String GRPC_FILE_NAME = "grpc.json";
    private static final String ROUTER_GRPC_FILE_NAME = "grpc_router.json";
    private static final String CRADLE_CONFIDENTIAL_FILE_NAME = "cradle.json";
    private static final String PROMETHEUS_FILE_NAME = "prometheus.json";
    private static final String CUSTOM_FILE_NAME = "custom.json";
    private static final String BOX_FILE_NAME = "box.json";
    private static final String CONNECTION_MANAGER_CONF_FILE_NAME = "mq_router.json";
    private static final String CRADLE_NON_CONFIDENTIAL_FILE_NAME = "cradle_manager.json";

    /** @deprecated please use {@link #DICTIONARY_ALIAS_DIR_NAME} */
    @Deprecated
    private static final String DICTIONARY_TYPE_DIR_NAME = "dictionary";
    private static final String DICTIONARY_ALIAS_DIR_NAME = "dictionaries";

    private static final String RABBITMQ_SECRET_NAME = "rabbitmq";
    private static final String CASSANDRA_SECRET_NAME = "cassandra";
    private static final String RABBITMQ_PASSWORD_KEY = "rabbitmq-password";
    private static final String CASSANDRA_PASSWORD_KEY = "cassandra-password";

    private static final String KEY_RABBITMQ_PASS = "RABBITMQ_PASS";
    private static final String KEY_CASSANDRA_PASS = "CASSANDRA_PASS";

    private static final String GENERATED_CONFIG_DIR_NAME = "generated_configs";

    private final Path custom;
    private final Path dictionaryTypesDir;
    private final Path dictionaryAliasesDir;
    private final Path oldDictionariesDir;
    private final ConfigurationManager configurationManager;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFactory.class.getName());

    protected CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                            Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
                            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                            Class<? extends GrpcRouter> grpcRouterClass,
                            @Nullable Path custom,
                            @Nullable Path dictionaryTypesDir,
                            @Nullable Path dictionaryAliasesDir,
                            @Nullable Path oldDictionariesDir,
                            Map<String, String> environmentVariables,
                            ConfigurationManager configurationManager) {
        super(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass, environmentVariables);

        this.custom = defaultPathIfNull(custom, CUSTOM_FILE_NAME);
        this.dictionaryTypesDir = defaultPathIfNull(dictionaryTypesDir, DICTIONARY_TYPE_DIR_NAME);
        this.dictionaryAliasesDir = defaultPathIfNull(dictionaryAliasesDir, DICTIONARY_ALIAS_DIR_NAME);
        this.oldDictionariesDir = requireNonNullElse(oldDictionariesDir, CONFIG_DEFAULT_PATH);
        this.configurationManager = configurationManager;

        start();
    }

    public CommonFactory(FactorySettings settings) {
        this(settings.getMessageRouterParsedBatchClass(),
                settings.getMessageRouterRawBatchClass(),
                settings.getMessageRouterMessageGroupBatchClass(),
                settings.getEventBatchRouterClass(),
                settings.getGrpcRouterClass(),
                settings.getCustom(),
                settings.getDictionaryTypesDir(),
                settings.getDictionaryAliasesDir(),
                settings.getOldDictionariesDir(),
                settings.getVariables(),
                createConfigurationManager(settings));
    }


    /**
     * @deprecated Please use {@link CommonFactory#CommonFactory(FactorySettings)}
     */
    @Deprecated(since = "3.10.0", forRemoval = true)
    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                         Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                         Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
                         Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                         Class<? extends GrpcRouter> grpcRouterClass,
                         Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {

        this(new FactorySettings(messageRouterParsedBatchClass,
                messageRouterRawBatchClass,
                messageRouterMessageGroupBatchClass,
                eventBatchRouterClass,
                grpcRouterClass,
                rabbitMQ,
                routerMQ,
                null,
                routerGRPC,
                null,
                cradle,
                null,
                prometheus,
                boxConfiguration,
                custom,
                dictionariesDir));
    }

    /**
     * @deprecated Please use {@link CommonFactory#CommonFactory(FactorySettings)}
     */
    @Deprecated(since = "3.10.0", forRemoval = true)
    public CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {
        this(RabbitParsedBatchRouter.class, RabbitRawBatchRouter.class, RabbitMessageGroupBatchRouter.class, EventBatchRouter.class, DefaultGrpcRouter.class,
                rabbitMQ ,routerMQ ,routerGRPC ,cradle ,custom ,dictionariesDir ,prometheus ,boxConfiguration);
    }

    /**
     * @deprecated Please use {@link CommonFactory#CommonFactory(FactorySettings)}
     */
    @Deprecated(since = "3.10.0", forRemoval = true)
    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                         Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                         Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
                         Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                         Class<? extends GrpcRouter> grpcRouterClass) {
        this(new FactorySettings(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass));
    }

    public CommonFactory() {
        this(new FactorySettings());
    }

    @Override
    protected Path getPathToCustomConfiguration() {
        return custom;
    }

    @Override
    protected Path getPathToDictionaryTypesDir() {
        return dictionaryTypesDir;
    }

    @Override
    protected Path getPathToDictionaryAliasesDir() {
        return dictionaryAliasesDir;
    }

    @Override
    protected Path getOldPathToDictionariesDir() {
        return oldDictionariesDir;
    }

    @Override
    protected ConfigurationManager getConfigurationManager() {
        return configurationManager;
    }
    /**
     * Create {@link CommonFactory} from command line arguments
     *
     * @param args String array of command line arguments. Arguments:
     *             <p>
     *             --rabbitConfiguration - path to json file with RabbitMQ configuration
     *             <p>
     *             --messageRouterConfiguration - path to json file with configuration for {@link MessageRouter}
     *             <p>
     *             --grpcRouterConfiguration - <b>Deprecated!!!</b> Please use <i>grpcConfiguration</i>!!! Path to json file with configuration for {@link GrpcRouter}
     *             <p>
     *             --grpcConfiguration - path to json file with configuration {@link GrpcConfiguration}
     *             <p>
     *             --grpcRouterConfig - path to json file with configuration {@link GrpcRouterConfiguration}
     *             <p>
     *             --cradleConfiguration - <b>Deprecated!!!</b> Please use <i>cradleConfidentialConfiguration</i>!!! Path to json file with configuration for cradle. ({@link CradleConfidentialConfiguration})
     *             <p>
     *             --cradleConfidentialConfiguration - path to json file with configuration for cradle. ({@link CradleConfidentialConfiguration})
     *             <p>
     *             --customConfiguration - path to json file with custom configuration
     *             <p>
     *             --dictionariesDir - path to directory which contains files with encoded dictionaries
     *             <p>
     *             --prometheusConfiguration - path to json file with configuration for prometheus metrics server
     *             <p>
     *             --boxConfiguration - path to json file with boxes configuration and information
     *             <p>
     *             --connectionManagerConfiguration - path to json file with for {@link ConnectionManagerConfiguration}
     *             <p>
     *             --cradleManagerConfiguration - path to json file with for {@link CradleNonConfidentialConfiguration}
     *             <p>
     *             --namespace - namespace in Kubernetes to find config maps related to the target
     *             <p>
     *             --boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
     *             <p>
     *             --contextName - context name to choose the context from Kube config
     *             <p>
     *             --dictionaries - which dictionaries will be use, and types for it (example: fix-50=main fix-55=level1)
     *      *      <p>
     *             -c/--configs - folder with json files for schemas configurations with special names:
     *             <p>
     *             rabbitMq.json - configuration for RabbitMQ
     *             mq.json - configuration for {@link MessageRouter}
     *             grpc.json - configuration for {@link GrpcRouter}
     *             cradle.json - configuration for cradle
     *             custom.json - custom configuration
     * @return CommonFactory with set path
     * @throws IllegalArgumentException - Cannot parse command line arguments
     */
    public static CommonFactory createFromArguments(String... args) {
        Options options = new Options();

        Option configOption = new Option("c", "configs", true, null);
        options.addOption(configOption);

        Option rabbitConfigurationOption = createLongOption(options, "rabbitConfiguration");
        Option messageRouterConfigurationOption = createLongOption(options, "messageRouterConfiguration");
        Option grpcRouterConfigurationOption = createLongOption(options, "grpcRouterConfiguration");
        Option grpcConfigurationOption = createLongOption(options, "grpcConfiguration");
        Option grpcRouterConfigOption = createLongOption(options, "grpcRouterConfig");
        Option cradleConfigurationOption = createLongOption(options, "cradleConfiguration");
        Option cradleConfidentialConfigurationOption = createLongOption(options, "cradleConfidentialConfiguration");
        Option customConfigurationOption = createLongOption(options, "customConfiguration");
        Option dictionariesDirOption = createLongOption(options, "dictionariesDir");
        Option prometheusConfigurationOption = createLongOption(options, "prometheusConfiguration");
        Option boxConfigurationOption = createLongOption(options, "boxConfiguration");
        Option namespaceOption = createLongOption(options, "namespace");
        Option boxNameOption = createLongOption(options, "boxName");
        Option contextNameOption = createLongOption(options, "contextName");
        Option dictionariesOption = createLongOption(options, "dictionaries");
        dictionariesOption.setArgs(Option.UNLIMITED_VALUES);
        Option connectionManagerConfigurationOption = createLongOption(options, "connectionManagerConfiguration");
        Option cradleManagerConfigurationOption = createLongOption(options, "cradleManagerConfiguration");

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);

            String configs = cmd.getOptionValue(configOption.getLongOpt());

            if (cmd.hasOption(namespaceOption.getLongOpt()) && cmd.hasOption(boxNameOption.getLongOpt())) {
                String namespace = cmd.getOptionValue(namespaceOption.getLongOpt());
                String boxName = cmd.getOptionValue(boxNameOption.getLongOpt());
                String contextName = cmd.getOptionValue(contextNameOption.getLongOpt());

                Map<DictionaryType, String> dictionaries = new HashMap<>();
                if (cmd.hasOption(dictionariesOption.getLongOpt())) {
                    for (String singleDictionary : cmd.getOptionValues(dictionariesOption.getLongOpt())) {
                        String[] keyValue = singleDictionary.split("=");

                        if (keyValue.length != 2 || StringUtils.isEmpty(keyValue[0].trim()) || StringUtils.isEmpty(keyValue[1].trim())) {
                            throw new IllegalStateException(String.format("Argument '%s' in '%s' option has wrong format.", singleDictionary, dictionariesOption.getLongOpt()));
                        }

                        String typeStr = keyValue[1].trim();

                        DictionaryType type;
                        try {
                            type = DictionaryType.valueOf(typeStr.toUpperCase());
                        } catch (IllegalArgumentException e) {
                            throw new IllegalStateException("Can not find dictionary type = " + typeStr, e);
                        }
                        dictionaries.put(type, keyValue[0].trim());
                    }
                }

                return createFromKubernetes(namespace, boxName, contextName, dictionaries);
            }

            if (configs != null) {
                configureLogger(configs);
            }
            FactorySettings settings = new FactorySettings();
            settings.setRabbitMQ(calculatePath(cmd, rabbitConfigurationOption, configs, RABBIT_MQ_FILE_NAME));
            settings.setRouterMQ(calculatePath(cmd, messageRouterConfigurationOption, configs, ROUTER_MQ_FILE_NAME));
            settings.setConnectionManagerSettings(calculatePath(cmd, connectionManagerConfigurationOption, configs, CONNECTION_MANAGER_CONF_FILE_NAME));
            settings.setGrpc(calculatePath(cmd, grpcConfigurationOption, grpcRouterConfigurationOption, configs, GRPC_FILE_NAME));
            settings.setRouterGRPC(calculatePath(cmd, grpcRouterConfigOption, configs, ROUTER_GRPC_FILE_NAME));
            settings.setCradleConfidential(calculatePath(cmd, cradleConfidentialConfigurationOption, cradleConfigurationOption, configs, CRADLE_CONFIDENTIAL_FILE_NAME));
            settings.setCradleNonConfidential(calculatePath(cmd, cradleManagerConfigurationOption, configs, CRADLE_NON_CONFIDENTIAL_FILE_NAME));
            settings.setPrometheus(calculatePath(cmd, prometheusConfigurationOption, configs, PROMETHEUS_FILE_NAME));
            settings.setBoxConfiguration(calculatePath(cmd, boxConfigurationOption, configs, BOX_FILE_NAME));
            settings.setCustom(calculatePath(cmd, customConfigurationOption, configs, CUSTOM_FILE_NAME));
            settings.setDictionaryTypesDir(calculatePath(cmd, dictionariesDirOption, configs, DICTIONARY_TYPE_DIR_NAME));
            settings.setDictionaryAliasesDir(calculatePath(cmd, dictionariesDirOption, configs, DICTIONARY_ALIAS_DIR_NAME));
            String oldDictionariesDir = cmd.getOptionValue(dictionariesDirOption.getLongOpt());
            settings.setOldDictionariesDir(oldDictionariesDir == null ? (configs == null ? CONFIG_DEFAULT_PATH : Path.of(configs)) : Path.of(oldDictionariesDir));

            return new CommonFactory(settings);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Incorrect arguments " + Arrays.toString(args), e);
        }
    }

    /**
     * Create {@link CommonFactory} via configs map from Kubernetes
     *
     * @param namespace - namespace in Kubernetes to find config maps related to the target th2 box
     * @param boxName   - the name of the target th2 box placed in the specified namespace in Kubernetes
     * @return CommonFactory with set path
     */
    public static CommonFactory createFromKubernetes(String namespace, String boxName) {
        return createFromKubernetes(namespace, boxName, null);
    }

    /**
     * Create {@link CommonFactory} via configs map from Kubernetes
     *
     * @param namespace   - namespace in Kubernetes to find config maps related to the target th2 box
     * @param boxName     - the name of the target th2 box placed in the specified namespace in Kubernetes
     * @param contextName - context name to choose the context from Kube config
     * @return CommonFactory with set path
     */
    public static CommonFactory createFromKubernetes(String namespace, String boxName, @Nullable String contextName) {
        return createFromKubernetes(namespace, boxName, contextName, emptyMap());
    }

    /**
     * Create {@link CommonFactory} via configs map from Kubernetes
     *
     * @param namespace - namespace in Kubernetes to find config maps related to the target th2 box
     * @param boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
     * @param contextName - context name to choose the context from Kube config
     * @param dictionaries - which dictionaries should load and type for ones.
     * @return CommonFactory with set path
     */
    public static CommonFactory createFromKubernetes(String namespace, String boxName, @Nullable String contextName, @NotNull Map<DictionaryType, String> dictionaries) {
        Resource<ConfigMap, DoneableConfigMap> boxConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> rabbitMqConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> cradleConfigMapResource;

        ConfigMap boxConfigMap;
        ConfigMap rabbitMqConfigMap;
        ConfigMap cradleConfigMap;

        Path configPath = Path.of(System.getProperty("user.dir"), GENERATED_CONFIG_DIR_NAME);

        Path dictionaryTypePath = configPath.resolve(DICTIONARY_TYPE_DIR_NAME);
        Path dictionaryAliasPath = configPath.resolve(DICTIONARY_ALIAS_DIR_NAME);

        Path boxConfigurationPath = configPath.resolve(BOX_FILE_NAME);

        FactorySettings settings = new FactorySettings();

        KubernetesClient client = contextName == null ? new DefaultKubernetesClient() : new DefaultKubernetesClient(Config.autoConfigure(contextName));

        try(client) {

            Secret rabbitMqSecret = requireNonNull(client.secrets().inNamespace(namespace).withName(RABBITMQ_SECRET_NAME).get(),
                    "Secret '" + RABBITMQ_SECRET_NAME + "' isn't found in namespace " + namespace);
            Secret cassandraSecret = requireNonNull(client.secrets().inNamespace(namespace).withName(CASSANDRA_SECRET_NAME).get(),
                    "Secret '" + CASSANDRA_SECRET_NAME + "' isn't found in namespace " + namespace);

            String encodedRabbitMqPass = requireNonNull(rabbitMqSecret.getData().get(RABBITMQ_PASSWORD_KEY),
                    "Key '" + RABBITMQ_PASSWORD_KEY + "' not found in secret '" + RABBITMQ_SECRET_NAME + "' in namespace " + namespace);
            String encodedCassandraPass = requireNonNull(cassandraSecret.getData().get(CASSANDRA_PASSWORD_KEY),
                    "Key '" + CASSANDRA_PASSWORD_KEY + "' not found in secret '" + CASSANDRA_SECRET_NAME + "' in namespace " + namespace);

            String rabbitMqPassword = new String(Base64.getDecoder().decode(encodedRabbitMqPass));
            String cassandraPassword = new String(Base64.getDecoder().decode(encodedCassandraPass));

            settings.putVariable(KEY_RABBITMQ_PASS, rabbitMqPassword);
            settings.putVariable(KEY_CASSANDRA_PASS, cassandraPassword);

            var configMaps = client.configMaps();

            boxConfigMapResource = configMaps.inNamespace(namespace).withName(boxName + "-app-config");
            rabbitMqConfigMapResource = configMaps.inNamespace(namespace).withName("rabbit-mq-external-app-config");
            cradleConfigMapResource = configMaps.inNamespace(namespace).withName("cradle-external");

            if (boxConfigMapResource.get() == null)
                throw new IllegalArgumentException("Failed to find config maps by boxName " + boxName);

            boxConfigMap = boxConfigMapResource.require();
            rabbitMqConfigMap = rabbitMqConfigMapResource.require();
            cradleConfigMap = cradleConfigMapResource.require();

            Map<String, String> boxData = boxConfigMap.getData();
            Map<String, String> rabbitMqData = rabbitMqConfigMap.getData();
            Map<String, String> cradleConfigData = cradleConfigMap.getData();

            File generatedConfigsDirFile = configPath.toFile();

            if (generatedConfigsDirFile.mkdir()) {
                LOGGER.info("Directory {} is created", configPath);
            } else {
                LOGGER.info("All boxConf in the '{}' folder are overridden", configPath);
            }

            if (generatedConfigsDirFile.exists()) {
                BoxConfiguration box = new BoxConfiguration();
                box.setBoxName(boxName);

                settings.setRabbitMQ(writeFile(configPath, RABBIT_MQ_FILE_NAME, rabbitMqData));
                settings.setRouterMQ(writeFile(configPath, ROUTER_MQ_FILE_NAME, boxData));
                settings.setConnectionManagerSettings(writeFile(configPath, CONNECTION_MANAGER_CONF_FILE_NAME, boxData));
                settings.setGrpc(writeFile(configPath, GRPC_FILE_NAME, boxData));
                settings.setRouterGRPC(writeFile(configPath, ROUTER_GRPC_FILE_NAME, boxData));
                settings.setCradleConfidential(writeFile(configPath, CRADLE_CONFIDENTIAL_FILE_NAME, cradleConfigData));
                settings.setCradleNonConfidential(writeFile(configPath, CRADLE_NON_CONFIDENTIAL_FILE_NAME, boxData));
                settings.setPrometheus(writeFile(configPath, PROMETHEUS_FILE_NAME, boxData));
                settings.setCustom(writeFile(configPath, CUSTOM_FILE_NAME, boxData));

                settings.setBoxConfiguration(boxConfigurationPath);
                settings.setDictionaryTypesDir(dictionaryTypePath);
                settings.setDictionaryAliasesDir(dictionaryAliasPath);

                String boxConfig = boxData.get(BOX_FILE_NAME);

                if (boxConfig != null)
                    writeFile(boxConfigurationPath, boxConfig);
                else
                    writeToJson(boxConfigurationPath, box);

                writeDictionaries(boxName, configPath, dictionaryTypePath, dictionaries, configMaps.list());
            }

            return new CommonFactory(settings);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public InputStream loadSingleDictionary() {
        Path dictionaryFolder = getPathToDictionaryAliasesDir();
        try {
            LOGGER.debug("Loading dictionary from folder: {}", dictionaryFolder);
            List<Path> dictionaries = null;
            if (Files.isDirectory(dictionaryFolder)) {
                try (Stream<Path> files = Files.list(dictionaryFolder)) {
                    dictionaries = files.filter(Files::isRegularFile).collect(Collectors.toList());
                }
            }

            if (dictionaries==null || dictionaries.isEmpty()) {
                throw new IllegalStateException("No dictionary at path: " + dictionaryFolder.toAbsolutePath());
            } else if (dictionaries.size() > 1) {
                throw new IllegalStateException("Found several dictionaries at path: " + dictionaryFolder.toAbsolutePath());
            }

            var targetDictionary = dictionaries.get(0);

            return new ByteArrayInputStream(getGzipBase64StringDecoder().decode(Files.readString(targetDictionary)));
        } catch (IOException e) {
            throw new IllegalStateException("Can not read dictionary from path: " + dictionaryFolder.toAbsolutePath(), e);
        }
    }

    @Override
    public Set<String> getDictionaryAliases() {
        Path dictionaryFolder = getPathToDictionaryAliasesDir();
        try {
            if (!Files.isDirectory(dictionaryFolder)) {
                return Set.of();
            }

            try (Stream<Path> files = Files.list(dictionaryFolder)) {
                return files
                        .filter(Files::isRegularFile)
                        .map(dictionary -> FilenameUtils.removeExtension(dictionary.getFileName().toString()))
                        .collect(Collectors.toSet());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Can not get dictionaries aliases from path: " + dictionaryFolder.toAbsolutePath(), e);
        }
    }

    @Override
    public Path getDictionaryPath(String alias) {
        Path dictionaryFolder = getPathToDictionaryAliasesDir();
        try {
            LOGGER.debug("Loading dictionary by alias ({}) from folder: {}", alias, dictionaryFolder);
            List<Path> dictionaries = null;

            if (Files.isDirectory(dictionaryFolder)) {
                try (Stream<Path> files = Files.list(dictionaryFolder)) {
                    dictionaries = files
                            .filter(Files::isRegularFile)
                            .filter(path -> FilenameUtils.removeExtension(path.getFileName().toString()).equalsIgnoreCase(alias))
                            .collect(Collectors.toList());
                }
            }

            if (dictionaries==null || dictionaries.isEmpty()) {
                throw new IllegalStateException("No dictionary was found by alias '" + alias + "' at path: " + dictionaryFolder.toAbsolutePath());
            } else if (dictionaries.size() > 1) {
                throw new IllegalStateException("Found several dictionaries by alias '" + alias + "' at path: " + dictionaryFolder.toAbsolutePath());
            }

            return dictionaries.get(0);
        } catch (IOException e) {
            throw new IllegalStateException("Can not read dictionary '" + alias + "' from path: " + dictionaryFolder.toAbsolutePath(), e);
        }
    }

    @Override
    public InputStream loadDictionary(String alias) {
        Path dictionaryPath = getDictionaryPath(alias);
        try {
            return new ByteArrayInputStream(getGzipBase64StringDecoder().decode(Files.readString(dictionaryPath)));
        } catch (IOException e) {
            throw new IllegalStateException("Can not read dictionary '" + alias + "' from path: " + dictionaryPath, e);
        }
    }

    @Override
    public InputStream readDictionary() {
        return readDictionary(DictionaryType.MAIN);
    }

    @Override
    public InputStream readDictionary(DictionaryType dictionaryType) {
        try {
            List<Path> dictionaries = null;
            Path typeFolder = dictionaryType.getDictionary(getPathToDictionaryTypesDir());
            if (Files.isDirectory(typeFolder)) {
                try (Stream<Path> files = Files.list(typeFolder)) {
                    dictionaries = files.filter(Files::isRegularFile)
                            .collect(Collectors.toList());
                }
            }

            // Find with old format
            Path oldFolder = getOldPathToDictionariesDir();
            if ((dictionaries == null || dictionaries.isEmpty()) && Files.isDirectory(oldFolder)) {
                try (Stream<Path> files = Files.list(oldFolder)) {
                    dictionaries = files.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().contains(dictionaryType.name()))
                            .collect(Collectors.toList());
                }
            }

            Path dictionaryAliasFolder = getPathToDictionaryAliasesDir();
            if ((dictionaries == null || dictionaries.isEmpty()) && Files.isDirectory(dictionaryAliasFolder)) {
                try (Stream<Path> files = Files.list(dictionaryAliasFolder)) {
                    dictionaries = files.filter(Files::isRegularFile).filter(path -> FilenameUtils.removeExtension(path.getFileName().toString()).equalsIgnoreCase(dictionaryType.name())).collect(Collectors.toList());
                }
            }

            if (dictionaries == null || dictionaries.isEmpty()) {
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

    private static Path writeFile(Path configPath, String fileName, Map<String, String> configMap) throws IOException {
        Path file = configPath.resolve(fileName);
        writeFile(file, configMap.get(fileName));
        return file;
    }

    private static void writeDictionaries(String boxName, Path oldDictionariesDir, Path dictionariesDir, Map<DictionaryType, String> dictionaries, ConfigMapList configMapList) throws IOException {
        for(ConfigMap configMap : configMapList.getItems()) {
            String configMapName = configMap.getMetadata().getName();
            if(configMapName.startsWith(boxName) && configMapName.endsWith("-dictionary")) {
                configMap.getData().forEach((fileName, base64) -> {
                    try {
                        writeFile(oldDictionariesDir.resolve(fileName), base64);
                    } catch (IOException e) {
                        LOGGER.error("Can not write dictionary '{}' from config map with name '{}'", fileName, configMapName);
                    }
                });
            }
        }

        for (Map.Entry<DictionaryType, String> entry : dictionaries.entrySet()) {
            DictionaryType type = entry.getKey();
            String dictionaryName = entry.getValue();
            for (ConfigMap dictionaryConfigMap : configMapList.getItems()) {
                String configName = dictionaryConfigMap.getMetadata().getName();
                if (configName.endsWith("-dictionary") && configName.substring(0, configName.lastIndexOf('-')).equals(dictionaryName)) {
                    Path dictionaryTypeDir = type.getDictionary(dictionariesDir);

                    if (Files.notExists(dictionaryTypeDir)) {
                        Files.createDirectories(dictionaryTypeDir);
                    } else if (!Files.isDirectory(dictionaryTypeDir)) {
                        throw new IllegalStateException(
                                String.format("Can not save dictionary '%s' with type '%s', because '%s' is not directory", dictionaryName, type, dictionaryTypeDir)
                        );
                    }

                    Set<String> fileNameSet = dictionaryConfigMap.getData().keySet();

                    if (fileNameSet.size() != 1) {
                        throw new IllegalStateException(
                                String.format("Can not save dictionary '%s' with type '%s', because can not find dictionary data in config map", dictionaryName, type)
                        );
                    }

                    String fileName = fileNameSet.stream().findFirst().orElse(null);
                    Path dictionaryPath = dictionaryTypeDir.resolve(fileName);
                    writeFile(dictionaryPath, dictionaryConfigMap.getData().get(fileName));
                    LOGGER.debug("Dictionary written in folder: " + dictionaryPath);
                    break;
                }
            }
        }
    }

    private static ConfigurationManager createConfigurationManager(FactorySettings settings) {
        Map<Class<?>, Path> paths = new HashMap<>();
        paths.put(RabbitMQConfiguration.class, defaultPathIfNull(settings.getRabbitMQ(), RABBIT_MQ_FILE_NAME));
        paths.put(MessageRouterConfiguration.class, defaultPathIfNull(settings.getRouterMQ(), ROUTER_MQ_FILE_NAME));
        paths.put(ConnectionManagerConfiguration.class, defaultPathIfNull(settings.getConnectionManagerSettings(), CONNECTION_MANAGER_CONF_FILE_NAME));
        paths.put(GrpcConfiguration.class, defaultPathIfNull(settings.getGrpc(), GRPC_FILE_NAME));
        paths.put(GrpcRouterConfiguration.class, defaultPathIfNull(settings.getRouterGRPC(), ROUTER_GRPC_FILE_NAME));
        paths.put(CradleConfidentialConfiguration.class, defaultPathIfNull(settings.getCradleConfidential(), CRADLE_CONFIDENTIAL_FILE_NAME));
        paths.put(CradleNonConfidentialConfiguration.class, defaultPathIfNull(settings.getCradleNonConfidential(), CRADLE_NON_CONFIDENTIAL_FILE_NAME));
        paths.put(PrometheusConfiguration.class, defaultPathIfNull(settings.getPrometheus(), PROMETHEUS_FILE_NAME));
        paths.put(BoxConfiguration.class, defaultPathIfNull(settings.getBoxConfiguration(), BOX_FILE_NAME));
        return new ConfigurationManager(paths);
    }

    private static Path defaultPathIfNull(Path path, String name) {
        return path == null ? CONFIG_DEFAULT_PATH.resolve(name) : path;
    }

    private static void writeFile(Path path, String data) throws IOException {
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            IOUtils.write(data, outputStream, Charsets.UTF_8);
        }
    }

    private static void writeToJson(Path path, Object object) throws IOException {
        File file = path.toFile();
        if (file.createNewFile() || file.exists()) {
            MAPPER.writeValue(file, object);
        }
    }

    private static Option createLongOption(Options options, String optionName) {
        Option option = new Option(null, optionName, true, null);
        options.addOption(option);
        return option;
    }

    private static Path calculatePath(String path, String configsPath, String fileName) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath, fileName) : CONFIG_DEFAULT_PATH.resolve(fileName));
    }

    private static Path calculatePath(CommandLine cmd, Option option, String configs, String fileName) {
        return calculatePath(cmd.getOptionValue(option.getLongOpt()), configs, fileName);
    }

    private static Path calculatePath(CommandLine cmd, Option current, Option deprecated, String configs, String fileName) {
        return calculatePath(defaultIfNull(cmd.getOptionValue(current.getLongOpt()), cmd.getOptionValue(deprecated.getLongOpt())), configs, fileName);
    }
}
