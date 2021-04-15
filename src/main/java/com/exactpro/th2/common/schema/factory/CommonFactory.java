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

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.configuration.ConfigurationManager;
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration;
import com.exactpro.th2.common.schema.event.EventBatchRouter;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.api.model.Secret;
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
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Default implementation for {@link AbstractCommonFactory}
 */
public class CommonFactory extends AbstractCommonFactory {

    public static final Path CONFIG_DEFAULT_PATH = Path.of("/var/th2/config/");

    public static final String RABBIT_MQ_FILE_NAME = "rabbitMQ.json";
    public static final String ROUTER_MQ_FILE_NAME = "mq.json";
    public static final String GRPC_FILE_NAME = "grpc.json";
    public static final String ROUTER_GRPC_FILE_NAME = "grpc_router.json";
    public static final String CRADLE_CONFIDENTIAL_FILE_NAME = "cradle.json";
    public static final String PROMETHEUS_FILE_NAME = "prometheus.json";
    public static final String CUSTOM_FILE_NAME = "custom.json";
    public static final String DICTIONARY_FILE_NAME = "dictionary.json";
    public static final String BOX_FILE_NAME = "box.json";
    public static final String CONNECTION_MANAGER_CONF_FILE_NAME = "mq_router.json";
    public static final String CRADLE_NON_CONFIDENTIAL_FILE_NAME = "cradle_manager.json";

    private static final String RABBITMQ_SECRET_NAME = "rabbitmq";
    private static final String CASSANDRA_SECRET_NAME = "cassandra";
    private static final String RABBITMQ_PASSWORD_KEY = "rabbitmq-password";
    private static final String CASSANDRA_PASSWORD_KEY = "cassandra-password";

    private static final String KEY_RABBITMQ_PASS = "RABBITMQ_PASS";
    private static final String KEY_CASSANDRA_PASS = "CASSANDRA_PASS";

    private final Path custom;
    private final Path dictionariesDir;
    private final ConfigurationManager configurationManager;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFactory.class.getName());

    protected CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                            Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
                            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                            Class<? extends GrpcRouter> grpcRouterClass,
                            Path custom,
                            Path dictionariesDir,
                            Map<String, String> environmentVariables,
                            ConfigurationManager configurationManager) {
        super(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass, environmentVariables);

        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
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
                settings.getDictionariesDir(),
                settings.getVariables(),
                settings.createConfigurationManager());
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
                dictionariesDir,
                emptyMap()));
    }

    /**
     * @deprecated Please use {@link CommonFactory#CommonFactory(FactorySettings)}
     */
    @Deprecated(since = "3.10.0", forRemoval = true)
    public CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {
        this(new FactorySettings(RabbitParsedBatchRouter.class,
                RabbitRawBatchRouter.class,
                RabbitMessageGroupBatchRouter.class,
                EventBatchRouter.class,
                DefaultGrpcRouter.class,
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
                dictionariesDir,
                emptyMap()));
    }

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
    protected Path getPathToDictionariesDir() {
        return dictionariesDir;
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
        Option connectionManagerConfigurationOption = createLongOption(options, "connectionManagerConfiguration");
        Option cradleManagerConfigurationOption = createLongOption(options, "cradleManagerConfiguration");

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);

            String configs = cmd.getOptionValue(configOption.getLongOpt());

            if (cmd.hasOption(namespaceOption.getLongOpt()) && cmd.hasOption(boxNameOption.getLongOpt())) {
                String namespace = cmd.getOptionValue(namespaceOption.getLongOpt());
                String boxName = cmd.getOptionValue(boxNameOption.getLongOpt());
                String contextName = cmd.getOptionValue(contextNameOption.getLongOpt());

                return createFromKubernetes(namespace, boxName, contextName);
            } else {

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
                settings.setDictionariesDir(calculatePath(cmd.getOptionValue(dictionariesDirOption.getLongOpt()), configs));

                return new CommonFactory(settings);
            }
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
        Resource<ConfigMap, DoneableConfigMap> boxConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> rabbitMqConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> cradleConfigMapResource;

        ConfigMap boxConfigMap;
        ConfigMap rabbitMqConfigMap;
        ConfigMap cradleConfigMap;

        String userDir = System.getProperty("user.dir");
        String generatedConfigsDir = "generated_configs";

        Path dictionaryPath = Path.of(userDir, generatedConfigsDir, DICTIONARY_FILE_NAME);
        Path boxConfigurationPath = Path.of(userDir, generatedConfigsDir, BOX_FILE_NAME);
        
        FactorySettings settings = new FactorySettings();

        try (KubernetesClient client = new DefaultKubernetesClient()) {

            if (contextName != null) {
                boolean foundContext = false;

                for (NamedContext context : client.getConfiguration().getContexts()) {
                    if (context.getName().equals(contextName)) {
                        client.getConfiguration().setCurrentContext(context);
                        foundContext = true;
                        break;
                    }
                }

                if (!foundContext)
                    throw new IllegalArgumentException("Failed to find context " + contextName);
            }

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

            Map<String, String> environmentVariables = new HashMap<>();
            environmentVariables.put(KEY_RABBITMQ_PASS, rabbitMqPassword);
            environmentVariables.put(KEY_CASSANDRA_PASS, cassandraPassword);

            settings.setVariables(environmentVariables);

            var configMaps = client.configMaps();

            boxConfigMapResource = configMaps.inNamespace(namespace).withName(boxName + "-app-config");
            rabbitMqConfigMapResource = configMaps.inNamespace(namespace).withName("rabbit-mq-external-app-config");
            cradleConfigMapResource = configMaps.inNamespace(namespace).withName("cradle-external");

            if (boxConfigMapResource.get() == null)
                throw new IllegalArgumentException("Failed to find config maps by boxName " + boxName);

            boxConfigMap = boxConfigMapResource.require();
            rabbitMqConfigMap = rabbitMqConfigMapResource.require();
            cradleConfigMap = cradleConfigMapResource.require();

            ConfigMap dictionaryConfigMap = getDictionary(boxName, client.configMaps().list());

            Map<String, String> boxData = boxConfigMap.getData();
            Map<String, String> rabbitMqData = rabbitMqConfigMap.getData();
            Map<String, String> cradleConfigData = cradleConfigMap.getData();

            File generatedConfigsDirFile = new File(userDir, generatedConfigsDir);

            if (generatedConfigsDirFile.mkdir()) {
                LOGGER.info("Directory {} is created at {}", generatedConfigsDir, userDir);
            } else {
                LOGGER.info("All boxConf in the '{}' folder are overridden", generatedConfigsDir);
            }

            if (generatedConfigsDirFile.exists()) {
                BoxConfiguration box = new BoxConfiguration();
                box.setBoxName(boxName);

                settings.setRabbitMQ(writeFile(userDir, generatedConfigsDir, RABBIT_MQ_FILE_NAME, rabbitMqData));
                settings.setRouterMQ(writeFile(userDir, generatedConfigsDir, ROUTER_MQ_FILE_NAME, boxData));
                settings.setConnectionManagerSettings(writeFile(userDir, generatedConfigsDir, CONNECTION_MANAGER_CONF_FILE_NAME, boxData));
                settings.setGrpc(writeFile(userDir, generatedConfigsDir, GRPC_FILE_NAME, boxData));
                settings.setRouterGRPC(writeFile(userDir, generatedConfigsDir, ROUTER_GRPC_FILE_NAME, boxData));
                settings.setCradleConfidential(writeFile(userDir, generatedConfigsDir, CRADLE_CONFIDENTIAL_FILE_NAME, cradleConfigData));
                settings.setCradleNonConfidential(writeFile(userDir, generatedConfigsDir, CRADLE_NON_CONFIDENTIAL_FILE_NAME, boxData));
                settings.setPrometheus(writeFile(userDir, generatedConfigsDir, PROMETHEUS_FILE_NAME, boxData));
                settings.setCustom(writeFile(userDir, generatedConfigsDir, CUSTOM_FILE_NAME, boxData));

                settings.setBoxConfiguration(boxConfigurationPath);
                settings.setDictionariesDir(dictionaryPath);

                String boxConfig = boxData.get(BOX_FILE_NAME);

                if (boxConfig != null)
                    writeFile(boxConfigurationPath, boxConfig);
                else
                    writeToJson(boxConfigurationPath, box);

                if (dictionaryConfigMap != null) {
                    writeToJson(dictionaryPath, dictionaryConfigMap.getData());
                }
            }

            return new CommonFactory(settings);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private static Path writeFile(String userDir, String generatedConfigsDir, String fileName, Map<String, String> configMap) throws IOException {
        Path file = Path.of(userDir, generatedConfigsDir, fileName);
        writeFile(file, configMap.get(fileName));
        return file;
    }

    private static ConfigMap getDictionary(String boxName, ConfigMapList configMapList) {
        for (ConfigMap c : configMapList.getItems()) {
            if (c.getMetadata().getName().startsWith(boxName) && c.getMetadata().getName().endsWith("-dictionary")) {
                return c;
            }
        }
        return null;
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

    private static Path calculatePath(String path, String configsPath) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath) : CONFIG_DEFAULT_PATH);
    }

    private static Path calculatePath(String path, String configsPath, String fileName) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath, fileName) : CONFIG_DEFAULT_PATH.resolve(fileName));
    }

    private static Path calculatePath(CommandLine cmd, Option option, String configs, String fileName) {
        return calculatePath(cmd.getOptionValue(option.getLongOpt()), configs, fileName);
    }

    private static Path calculatePath(CommandLine cmd, Option current, Option deprecated, String configs, String fileName) {
        return calculatePath(ObjectUtils.defaultIfNull(cmd.getOptionValue(current.getLongOpt()), cmd.getOptionValue(deprecated.getLongOpt())), configs, fileName);
    }
}
