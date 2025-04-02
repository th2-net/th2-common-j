/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.configuration.ConfigurationManager;
import com.exactpro.th2.common.schema.configuration.IConfigurationProvider;
import com.exactpro.th2.common.schema.configuration.IDictionaryProvider;
import com.exactpro.th2.common.schema.configuration.impl.DictionaryKind;
import com.exactpro.th2.common.schema.configuration.impl.DictionaryProvider;
import com.exactpro.th2.common.schema.configuration.impl.JsonConfigurationProvider;
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Default implementation for {@link AbstractCommonFactory}
 */
public class CommonFactory extends AbstractCommonFactory {

    public static final String TH2_COMMON_SYSTEM_PROPERTY = "th2.common";
    public static final String TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY = TH2_COMMON_SYSTEM_PROPERTY + '.' + "configuration-directory";
    static final Path CONFIG_DEFAULT_PATH = Path.of("/var/th2/config/");

    static final String RABBIT_MQ_CFG_ALIAS = "rabbitMQ";
    static final String ROUTER_MQ_CFG_ALIAS = "mq";
    static final String GRPC_CFG_ALIAS = "grpc";
    static final String ROUTER_GRPC_CFG_ALIAS = "grpc_router";
    static final String CRADLE_CONFIDENTIAL_CFG_ALIAS = "cradle";
    static final String PROMETHEUS_CFG_ALIAS = "prometheus";
    static final String BOX_CFG_ALIAS = "box";
    static final String CONNECTION_MANAGER_CFG_ALIAS = "mq_router";
    static final String CRADLE_NON_CONFIDENTIAL_CFG_ALIAS = "cradle_manager";

    /** @deprecated please use {@link #DICTIONARY_ALIAS_DIR_NAME} */
    @Deprecated
    static final String DICTIONARY_TYPE_DIR_NAME = "dictionary";
    static final String DICTIONARY_ALIAS_DIR_NAME = "dictionaries";

    private static final String RABBITMQ_SECRET_NAME = "rabbitmq";
    private static final String CASSANDRA_SECRET_NAME = "cassandra";
    private static final String RABBITMQ_PASSWORD_KEY = "rabbitmq-password";
    private static final String CASSANDRA_PASSWORD_KEY = "cassandra-password";

    private static final String KEY_RABBITMQ_PASS = "RABBITMQ_PASS";
    private static final String KEY_CASSANDRA_PASS = "CASSANDRA_PASS";

    private static final String GENERATED_CONFIG_DIR_NAME = "generated_configs";
    private static final String RABBIT_MQ_EXTERNAL_APP_CONFIG_MAP = "rabbit-mq-external-app-config";
    private static final String CRADLE_EXTERNAL_MAP = "cradle-external";
    private static final String CRADLE_MANAGER_CONFIG_MAP = "cradle-manager";
    private static final String LOGGING_CONFIG_MAP = "logging-config";

    final IConfigurationProvider configurationProvider;
    final IDictionaryProvider dictionaryProvider;
    final ConfigurationManager configurationManager;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFactory.class.getName());

    private CommonFactory(@NotNull IConfigurationProvider configurationProvider, @NotNull IDictionaryProvider dictionaryProvider) {
        super();
        this.dictionaryProvider = requireNonNull(dictionaryProvider, "Dictionary provider can't be null");
        this.configurationProvider = requireNonNull(configurationProvider, "Configuration provider can't be null");
        configurationManager = createConfigurationManager(configurationProvider);
        start();
    }

    @Deprecated(since = "6", forRemoval = true)
    public CommonFactory(FactorySettings settings) {
        this(createConfigurationProvider(settings), createDictionaryProvider(settings));
    }

    public CommonFactory() {
        this(new FactorySettings());
    }

    @Override
    protected ConfigurationManager getConfigurationManager() {
        return configurationManager;
    }

    public IConfigurationProvider getConfigurationProvider() {
        return configurationProvider;
    }

    public static CommonFactory createFromProvider(@NotNull IConfigurationProvider configurationProvider,
                                                   @NotNull IDictionaryProvider dictionaryProvider) {
        return new CommonFactory(configurationProvider, dictionaryProvider);
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
     *             --cradleManagerConfiguration - path to json file with for {@link CradleNonConfidentialConfiguration} and {@link CassandraStorageSettings}
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

            Path configs = toPath(cmd.getOptionValue(configOption.getLongOpt()));

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

            FactorySettings settings = new FactorySettings();
            if (configs != null) {
                settings.setBaseConfigDir(configs);
                if (!CONFIG_DEFAULT_PATH.equals(configs)) {
                    configureLogger(configs);
                }
            }
            settings.setRabbitMQ(toPath(cmd.getOptionValue(rabbitConfigurationOption.getLongOpt())));
            settings.setRouterMQ(toPath(cmd.getOptionValue(messageRouterConfigurationOption.getLongOpt())));
            settings.setConnectionManagerSettings(toPath(cmd.getOptionValue(connectionManagerConfigurationOption.getLongOpt())));
            settings.setGrpc(toPath(defaultIfNull(cmd.getOptionValue(grpcConfigurationOption.getLongOpt()), cmd.getOptionValue(grpcRouterConfigurationOption.getLongOpt()))));
            settings.setRouterGRPC(toPath(cmd.getOptionValue(grpcRouterConfigOption.getLongOpt())));
            settings.setCradleConfidential(toPath(cmd.getOptionValue(cradleConfidentialConfigurationOption.getLongOpt())));
            settings.setCradleNonConfidential(toPath(defaultIfNull(cmd.getOptionValue(cradleManagerConfigurationOption.getLongOpt()), cmd.getOptionValue(cradleConfigurationOption.getLongOpt()))));
            settings.setPrometheus(toPath(cmd.getOptionValue(prometheusConfigurationOption.getLongOpt())));
            settings.setBoxConfiguration(toPath(cmd.getOptionValue(boxConfigurationOption.getLongOpt())));
            settings.setCustom(toPath(cmd.getOptionValue(customConfigurationOption.getLongOpt())));
            settings.setDictionaryTypesDir(toPath(cmd.getOptionValue(dictionariesDirOption.getLongOpt())));
            settings.setDictionaryAliasesDir(toPath(cmd.getOptionValue(dictionariesDirOption.getLongOpt())));
            settings.setOldDictionariesDir(toPath(cmd.getOptionValue(dictionariesDirOption.getLongOpt())));

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
    @Deprecated(since = "6", forRemoval = true)
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
    @Deprecated(since = "6", forRemoval = true)
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

        Path configPath = Path.of(System.getProperty("user.dir"), GENERATED_CONFIG_DIR_NAME);

        Path dictionaryTypePath = configPath.resolve(DICTIONARY_TYPE_DIR_NAME);
        Path dictionaryAliasPath = configPath.resolve(DICTIONARY_ALIAS_DIR_NAME);

        Path boxConfigurationPath = configPath.resolve(BOX_CFG_ALIAS);

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

            Resource<ConfigMap> boxConfigMapResource = configMaps.inNamespace(namespace).withName(boxName + "-app-config");

            if (boxConfigMapResource.get() == null) {
                throw new IllegalArgumentException("Failed to find config maps by boxName " + boxName);
            }
            Resource<ConfigMap> rabbitMqConfigMapResource = configMaps.inNamespace(namespace).withName(RABBIT_MQ_EXTERNAL_APP_CONFIG_MAP);
            Resource<ConfigMap> cradleConfidentialConfigMapResource = configMaps.inNamespace(namespace).withName(CRADLE_EXTERNAL_MAP);
            Resource<ConfigMap> cradleNonConfidentialConfigMapResource = configMaps.inNamespace(namespace).withName(CRADLE_MANAGER_CONFIG_MAP);
            Resource<ConfigMap> loggingConfigMapResource = configMaps.inNamespace(namespace).withName(LOGGING_CONFIG_MAP);

            ConfigMap boxConfigMap = boxConfigMapResource.require();
            ConfigMap rabbitMqConfigMap = rabbitMqConfigMapResource.require();
            ConfigMap cradleConfidentialConfigmap = cradleConfidentialConfigMapResource.require();
            ConfigMap cradleNonConfidentialConfigmap = cradleNonConfidentialConfigMapResource.require();
            @Nullable ConfigMap loggingConfigMap = loggingConfigMapResource.get();

            Map<String, String> boxData = boxConfigMap.getData();
            Map<String, String> rabbitMqData = rabbitMqConfigMap.getData();
            Map<String, String> cradleConfidential = cradleConfidentialConfigmap.getData();
            Map<String, String> cradleNonConfidential = cradleNonConfidentialConfigmap.getData();
            @Nullable String loggingData = boxData.getOrDefault(LOG4J2_PROPERTIES_NAME,
                    loggingConfigMap == null ? null : loggingConfigMap.getData().get(LOG4J2_PROPERTIES_NAME)
            );

            File generatedConfigsDirFile = configPath.toFile();

            if (generatedConfigsDirFile.mkdir()) {
                LOGGER.info("Directory {} is created", configPath);
            } else {
                LOGGER.info("All boxConf in the '{}' folder are overridden", configPath);
            }

            if (generatedConfigsDirFile.exists()) {
                BoxConfiguration box = new BoxConfiguration();
                box.setBoxName(boxName);

                if (loggingData != null) {
                    writeFile(configPath.resolve(LOG4J2_PROPERTIES_NAME), loggingData);
                    configureLogger(configPath);
                }

                settings.setRabbitMQ(writeFile(configPath, RABBIT_MQ_CFG_ALIAS, rabbitMqData));
                settings.setRouterMQ(writeFile(configPath, ROUTER_MQ_CFG_ALIAS, boxData));
                settings.setConnectionManagerSettings(writeFile(configPath, CONNECTION_MANAGER_CFG_ALIAS, boxData));
                settings.setGrpc(writeFile(configPath, GRPC_CFG_ALIAS, boxData));
                settings.setRouterGRPC(writeFile(configPath, ROUTER_GRPC_CFG_ALIAS, boxData));
                settings.setCradleConfidential(writeFile(configPath, CRADLE_CONFIDENTIAL_CFG_ALIAS, cradleConfidential));
                settings.setCradleNonConfidential(writeFile(configPath, CRADLE_NON_CONFIDENTIAL_CFG_ALIAS, cradleNonConfidential));
                settings.setPrometheus(writeFile(configPath, PROMETHEUS_CFG_ALIAS, boxData));
                settings.setCustom(writeFile(configPath, CUSTOM_CFG_ALIAS, boxData));

                settings.setBoxConfiguration(boxConfigurationPath);
                settings.setDictionaryTypesDir(dictionaryTypePath);
                settings.setDictionaryAliasesDir(dictionaryAliasPath);

                String boxConfig = boxData.get(BOX_CFG_ALIAS);

                if (boxConfig == null) {
                    writeToJson(boxConfigurationPath, box);
                } else {
                    writeFile(boxConfigurationPath, boxConfig);
                }

                writeDictionaries(dictionaryTypePath, dictionaryAliasPath, dictionaries, configMaps.list());
            }

            return new CommonFactory(settings);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public InputStream loadSingleDictionary() {
        return dictionaryProvider.load();
    }

    @Override
    public Set<String> getDictionaryAliases() {
        return dictionaryProvider.aliases();
    }

    @Override
    public InputStream loadDictionary(String alias) {
        return dictionaryProvider.load(alias);
    }

    @Override
    public InputStream readDictionary() {
        return dictionaryProvider.load(DictionaryType.MAIN);
    }

    @Override
    public InputStream readDictionary(DictionaryType dictionaryType) {
        return dictionaryProvider.load(dictionaryType);
    }

    static @NotNull Path getConfigPath() {
        return getConfigPath(null);
    }

    /**
     * Priority:
     *   1. passed via commandline arguments
     *   2. {@value #TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY} system property
     *   3. {@link #CONFIG_DEFAULT_PATH} default value
     */
    static @NotNull Path getConfigPath(@Nullable Path basePath) {
        if (basePath != null) {
            if (Files.exists(basePath) && Files.isDirectory(basePath)) {
                return basePath;
            }
            LOGGER.warn("'{}' config directory passed via CMD doesn't exist or it is not a directory", basePath);
        } else {
            LOGGER.debug("Skipped blank CMD path for configs directory");
        }

        String pathString = System.getProperty(TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY);
        if (pathString != null) {
            Path path = Paths.get(pathString);
            if (Files.exists(path) && Files.isDirectory(path)) {
                return path;
            }
            LOGGER.warn("'{}' config directory passed via '{}' system property doesn't exist or it is not a directory",
                    pathString,
                    TH2_COMMON_CONFIGURATION_DIRECTORY_SYSTEM_PROPERTY);
        } else {
            LOGGER.debug("Skipped blank environment variable path for configs directory");
        }

        if (!Files.exists(CONFIG_DEFAULT_PATH)) {
            LOGGER.error("'{}' default config directory doesn't exist", CONFIG_DEFAULT_PATH);
        }
        return CONFIG_DEFAULT_PATH;
    }

    private static Path writeFile(Path configPath, String fileName, Map<String, String> configMap) throws IOException {
        Path file = configPath.resolve(fileName);
        writeFile(file, configMap.get(fileName));
        return file;
    }

    private static void writeDictionaries(Path oldDictionariesDir, Path dictionariesDir, Map<DictionaryType, String> dictionaries, ConfigMapList configMapList) throws IOException {
        createDirectory(dictionariesDir);

        for(ConfigMap configMap : configMapList.getItems()) {
            String configMapName = configMap.getMetadata().getName();

            if (!configMapName.endsWith("-dictionary")) {
                continue;
            }

            String dictionaryName = configMapName.substring(0, configMapName.lastIndexOf('-'));

            dictionaries.entrySet().stream()
                    .filter(entry -> Objects.equals(entry.getValue(), dictionaryName))
                    .forEach(entry -> {
                        DictionaryType type = entry.getKey();
                        try {
                            Path dictionaryTypeDir = type.getDictionary(oldDictionariesDir);
                            createDirectory(dictionaryTypeDir);

                            if (configMap.getData().size() != 1) {
                                throw new IllegalStateException(
                                        String.format("Can not save dictionary '%s' with type '%s', because can not find dictionary data in config map", dictionaryName, type)
                                );
                            }

                            downloadFiles(dictionariesDir, configMap);
                            downloadFiles(dictionaryTypeDir, configMap);
                        } catch (Exception e) {
                            throw new IllegalStateException("Loading the " + dictionaryName + " dictionary with type " + type + " failures", e);
                        }
                    });
        }
    }

    private static void downloadFiles(Path baseDir, ConfigMap configMap) {
        String configMapName = configMap.getMetadata().getName();
        configMap.getData().forEach((fileName, base64) -> {
            try {
                Path path = baseDir.resolve(fileName);
                if (!Files.exists(path)) {
                    writeFile(path, base64);
                    LOGGER.info("The '{}' config has been downloaded from the '{}' config map to the '{}' path", fileName, configMapName, path);
                }
            } catch (IOException e) {
                LOGGER.error("Can not download the '{}' file from the '{}' config map", fileName, configMapName);
            }
        });
    }

    private static void createDirectory(Path dir) throws IOException {
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        } else if (!Files.isDirectory(dir)) {
            throw new IllegalStateException("Can not save dictionary '" + dir + "' because the '" + dir + "' has already exist and isn't a directory");
        }
    }

    private static IDictionaryProvider createDictionaryProvider(FactorySettings settings) {
        Map<DictionaryKind, Path> paths = new EnumMap<>(DictionaryKind.class);
        putIfNotNull(paths, DictionaryKind.OLD, settings.getOldDictionariesDir());
        putIfNotNull(paths, DictionaryKind.TYPE, settings.getDictionaryTypesDir());
        putIfNotNull(paths, DictionaryKind.ALIAS, settings.getDictionaryAliasesDir());
        return new DictionaryProvider(defaultIfNull(settings.getBaseConfigDir(), getConfigPath()), paths);
    }

    private static IConfigurationProvider createConfigurationProvider(FactorySettings settings) {
        Map<String, Path> paths = new HashMap<>();
        putIfNotNull(paths, CUSTOM_CFG_ALIAS, settings.getCustom());
        putIfNotNull(paths, RABBIT_MQ_CFG_ALIAS, settings.getRabbitMQ());
        putIfNotNull(paths, ROUTER_MQ_CFG_ALIAS, settings.getRouterMQ());
        putIfNotNull(paths, CONNECTION_MANAGER_CFG_ALIAS, settings.getConnectionManagerSettings());
        putIfNotNull(paths, GRPC_CFG_ALIAS, settings.getGrpc());
        putIfNotNull(paths, ROUTER_GRPC_CFG_ALIAS, settings.getRouterGRPC());
        putIfNotNull(paths, CRADLE_CONFIDENTIAL_CFG_ALIAS, settings.getCradleConfidential());
        putIfNotNull(paths, CRADLE_NON_CONFIDENTIAL_CFG_ALIAS, settings.getCradleNonConfidential());
        putIfNotNull(paths, PROMETHEUS_CFG_ALIAS, settings.getPrometheus());
        putIfNotNull(paths, BOX_CFG_ALIAS, settings.getBoxConfiguration());
        return new JsonConfigurationProvider(defaultIfNull(settings.getBaseConfigDir(), getConfigPath()), paths);
    }
    private static ConfigurationManager createConfigurationManager(IConfigurationProvider configurationProvider) {
        Map<Class<?>, String> paths = new HashMap<>();
        paths.put(RabbitMQConfiguration.class, RABBIT_MQ_CFG_ALIAS);
        paths.put(MessageRouterConfiguration.class, ROUTER_MQ_CFG_ALIAS);
        paths.put(ConnectionManagerConfiguration.class, CONNECTION_MANAGER_CFG_ALIAS);
        paths.put(GrpcConfiguration.class, GRPC_CFG_ALIAS);
        paths.put(GrpcRouterConfiguration.class, ROUTER_GRPC_CFG_ALIAS);
        paths.put(CradleConfidentialConfiguration.class, CRADLE_CONFIDENTIAL_CFG_ALIAS);
        paths.put(CradleNonConfidentialConfiguration.class, CRADLE_NON_CONFIDENTIAL_CFG_ALIAS);
        paths.put(CassandraStorageSettings.class, CRADLE_NON_CONFIDENTIAL_CFG_ALIAS);
        paths.put(PrometheusConfiguration.class, PROMETHEUS_CFG_ALIAS);
        paths.put(BoxConfiguration.class, BOX_CFG_ALIAS);
        return new ConfigurationManager(configurationProvider, paths);
    }

    private static <T> void putIfNotNull(@NotNull Map<T, Path> paths, @NotNull T key, @Nullable Path path) {
        requireNonNull(paths, "'Paths' can't be null");
        requireNonNull(key, "'Key' can't be null");
        if (path != null) {
            paths.put(key, path);
        }
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

    private static @Nullable Path toPath(@Nullable String path) {
        return path == null ? null : Path.of(path);
    }

}
