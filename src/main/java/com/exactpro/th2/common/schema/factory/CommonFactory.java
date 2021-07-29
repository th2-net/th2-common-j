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

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNullElse;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.dictionary.DictionaryType;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import kotlin.text.Charsets;

/**
 * Default implementation for {@link AbstractCommonFactory}
 */
public class CommonFactory extends AbstractCommonFactory {

    private static final Path CONFIG_DEFAULT_PATH = Path.of("/var/th2/config/");

    private static final String RABBIT_MQ_FILE_NAME = "rabbitMQ.json";
    private static final String ROUTER_MQ_FILE_NAME = "mq.json";
    private static final String ROUTER_GRPC_FILE_NAME = "grpc.json";
    private static final String CRADLE_FILE_NAME = "cradle.json";
    private static final String PROMETHEUS_FILE_NAME = "prometheus.json";
    private static final String CUSTOM_FILE_NAME = "custom.json";

    private static final String DICTIONARY_DIR_NAME = "dictionary";
    private static final String GENERATED_CONFIG_DIR_NAME = "generated_configs";

    private final Path rabbitMQ;
    private final Path routerMQ;
    private final Path routerGRPC;
    private final Path prometheus;
    private final Path cradle;
    private final Path custom;
    private final Path dictionariesDir;
    private final Path oldDictionariesDir;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFactory.class.getName());

    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            Class<? extends GrpcRouter> grpcRouterClass,
            Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, @Nullable Path oldDictionariesDir) {
        super(messageRouterParsedBatchClass, messageRouterRawBatchClass, eventBatchRouterClass, grpcRouterClass);
        this.rabbitMQ = rabbitMQ;
        this.routerMQ = routerMQ;
        this.routerGRPC = routerGRPC;
        this.cradle = cradle;
        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
        this.oldDictionariesDir = requireNonNullElse(oldDictionariesDir, CONFIG_DEFAULT_PATH);
        this.prometheus = prometheus;
    }

    public CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, @Nullable Path oldDictionariesDir) {
        this.rabbitMQ = rabbitMQ;
        this.routerMQ = routerMQ;
        this.routerGRPC = routerGRPC;
        this.cradle = cradle;
        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
        this.oldDictionariesDir = requireNonNullElse(oldDictionariesDir, CONFIG_DEFAULT_PATH);
        this.prometheus = prometheus;

        start();
    }

    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                         Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                         Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                         Class<? extends GrpcRouter> grpcRouterClass) {
        this(messageRouterParsedBatchClass, messageRouterRawBatchClass, eventBatchRouterClass, grpcRouterClass,
                CONFIG_DEFAULT_PATH.resolve(RABBIT_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_GRPC_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CRADLE_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CUSTOM_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(PROMETHEUS_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(DICTIONARY_DIR_NAME),
                CONFIG_DEFAULT_PATH
        );
    }

    public CommonFactory() {
        this(CONFIG_DEFAULT_PATH.resolve(RABBIT_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_GRPC_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CRADLE_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CUSTOM_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(PROMETHEUS_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(DICTIONARY_DIR_NAME),
                CONFIG_DEFAULT_PATH
        );
    }

    @Override
    protected Path getPathToRabbitMQConfiguration() {
        return rabbitMQ;
    }

    @Override
    protected Path getPathToMessageRouterConfiguration() {
        return routerMQ;
    }

    @Override
    protected Path getPathToGrpcRouterConfiguration() {
        return routerGRPC;
    }

    @Override
    protected Path getPathToCradleConfiguration() {
        return cradle;
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
    protected Path getOldPathToDictionariesDir() {
        return oldDictionariesDir;
    }

    @Override
    protected Path getPathToPrometheusConfiguration() {
        return prometheus;
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
     *             --grpcRouterConfiguration - path to json file with configuration for {@link GrpcRouter}
     *             <p>
     *             --cradleConfiguration - path to json file with configuration for cradle. ({@link CradleConfiguration})
     *             <p>
     *             --customConfiguration - path to json file with custom configuration
     *             <p>
     *             --dictionariesDir - path to directory which contains files with encoded dictionaries
     *             <p>
     *             --prometheusConfiguration - path to json file with configuration for prometheus metrics server
     *             <p>
     *             --namespace - namespace in Kubernetes to find config maps related to the target
     *             <p>
     *             --boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
     *             <p>
     *             --dictionaries - which dictionaries will be use, and types for it (example: fix-50=main;fix-55=level1)
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

        Option dictionariesDirOption = new Option(null, "dictionariesDir", true, null);
        Option dictionariesOption = new Option(null, "dictionaries", true, null);
        dictionariesOption.setArgs(Option.UNLIMITED_VALUES);

        options.addOption(new Option(null, "rabbitConfiguration", true, null));
        options.addOption(new Option(null, "messageRouterConfiguration", true, null));
        options.addOption(new Option(null, "grpcRouterConfiguration", true, null));
        options.addOption(new Option(null, "cradleConfiguration", true, null));
        options.addOption(new Option(null, "customConfiguration", true, null));
        options.addOption(dictionariesDirOption);
        options.addOption(new Option(null, "prometheusConfiguration", true, null));
        options.addOption(new Option("c", "configs", true, null));
        options.addOption(dictionariesOption);
        options.addOption(new Option(null, "namespace", true, null));
        options.addOption(new Option(null, "boxName", true, null));

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);

            String configsPath = cmd.getOptionValue("configs");

            if(cmd.hasOption("namespace") && cmd.hasOption("boxName")) {
                String namespace = cmd.getOptionValue("namespace");
                String boxName = cmd.getOptionValue("boxName");

                Map<DictionaryType, String> dictionaries = new HashMap<>();
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

                return createFromKubernetes(namespace, boxName, dictionaries);
            } else {
                if (configsPath != null) {
                    configureLogger(configsPath);
                }

                String oldDictionariesDir = cmd.getOptionValue(dictionariesDirOption.getLongOpt());

                return new CommonFactory(
                        calculatePath(cmd.getOptionValue("rabbitConfiguration"), configsPath, RABBIT_MQ_FILE_NAME),
                        calculatePath(cmd.getOptionValue("messageRouterConfiguration"), configsPath, ROUTER_MQ_FILE_NAME),
                        calculatePath(cmd.getOptionValue("grpcRouterConfiguration"), configsPath, ROUTER_GRPC_FILE_NAME),
                        calculatePath(cmd.getOptionValue("cradleConfiguration"), configsPath, CRADLE_FILE_NAME),
                        calculatePath(cmd.getOptionValue("customConfiguration"), configsPath, CUSTOM_FILE_NAME),
                        calculatePath(cmd.getOptionValue("prometheusConfiguration"), configsPath, PROMETHEUS_FILE_NAME),
                        calculatePath(cmd.getOptionValue("dictionariesDir"), configsPath, DICTIONARY_DIR_NAME),
                        oldDictionariesDir == null ? (configsPath == null ? CONFIG_DEFAULT_PATH : Path.of(configsPath)) : Path.of(oldDictionariesDir)
                );
            }
        } catch (ParseException e) {
            throw new IllegalArgumentException("Incorrect arguments " + Arrays.toString(args), e);
        }
    }

    /**
     * Create {@link CommonFactory} via configs map from Kubernetes
     *
     * @param namespace - namespace in Kubernetes to find config maps related to the target th2 box
     * @param boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
     * @return CommonFactory with set path
     */
    public static CommonFactory createFromKubernetes(String namespace, String boxName) {
        return createFromKubernetes(namespace, boxName, emptyMap());
    }

    /**
     * Create {@link CommonFactory} via configs map from Kubernetes
     *
     * @param namespace - namespace in Kubernetes to find config maps related to the target th2 box
     * @param boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
     * @param dictionaries - which dictionaries should load and type for ones.
     * @return CommonFactory with set path
     */
    public static CommonFactory createFromKubernetes(String namespace, String boxName, @NotNull Map<DictionaryType, String> dictionaries) {
        Resource<ConfigMap, DoneableConfigMap> boxConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> rabbitMqConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> cradleConfigMapResource;

        ConfigMap boxConfigMap;
        ConfigMap rabbitMqConfigMap;
        ConfigMap cradleConfigMap;

        Path configPath = Path.of(System.getProperty("user.dir"), GENERATED_CONFIG_DIR_NAME);

        Path grpcPath = configPath.resolve(ROUTER_GRPC_FILE_NAME);
        Path rabbitMqPath = configPath.resolve(RABBIT_MQ_FILE_NAME);
        Path cradlePath = configPath.resolve(CRADLE_FILE_NAME);
        Path mqPath = configPath.resolve(ROUTER_MQ_FILE_NAME);
        Path customPath = configPath.resolve(CUSTOM_FILE_NAME);
        Path prometheusPath = configPath.resolve(PROMETHEUS_FILE_NAME);
        Path dictionaryPath = configPath.resolve(DICTIONARY_DIR_NAME);

        try (KubernetesClient client = new DefaultKubernetesClient()) {

            String currentCluster = client.getConfiguration().getCurrentContext().getContext().getCluster();
            String username = client.getConfiguration().getCurrentContext().getContext().getUser();

            var configMaps
                    = client.configMaps();
            var services
                    = client.services();

            boxConfigMapResource = configMaps.inNamespace(namespace).withName(boxName + "-app-config");
            rabbitMqConfigMapResource = configMaps.inNamespace(namespace).withName("rabbit-mq-app-config");
            cradleConfigMapResource = configMaps.inNamespace(namespace).withName("cradle");

            boxConfigMap = boxConfigMapResource.require();
            rabbitMqConfigMap = rabbitMqConfigMapResource.require();
            cradleConfigMap = cradleConfigMapResource.require();

            Map<String, String> boxData = boxConfigMap.getData();
            Map<String, String> rabbitMqData = rabbitMqConfigMap.getData();
            Map<String, String> cradleConfigData = cradleConfigMap.getData();

            Service rabbitMqService = services.inNamespace("service").withName("rabbitmq-schema").require();
            Service cassandraService = services.inNamespace("service").withName("cassandra-schema").require();

            GrpcRouterConfiguration grpc = MAPPER.readValue(boxData.get(ROUTER_GRPC_FILE_NAME), GrpcRouterConfiguration.class);
            MessageRouterConfiguration mq = MAPPER.readValue(boxData.get(ROUTER_MQ_FILE_NAME), MessageRouterConfiguration.class);
            ObjectNode custom = MAPPER.readValue(boxData.get(CUSTOM_FILE_NAME), ObjectNode.class);
            RabbitMQConfiguration rabbitMq = MAPPER.readValue(rabbitMqData.get(RABBIT_MQ_FILE_NAME), RabbitMQConfiguration.class);
            CradleConfiguration cradle = MAPPER.readValue(cradleConfigData.get(CRADLE_FILE_NAME), CradleConfiguration.class);

            Map<String, GrpcServiceConfiguration> grpcServices = grpc.getServices();

            grpcServices.values().forEach(serviceConfig -> serviceConfig.getEndpoints().values().forEach(endpoint -> {
                Service boxService = client.services().inNamespace(namespace).withName(endpoint.getHost()).require();
                endpoint.setPort(getExposedPort(endpoint.getPort(), boxService));
                endpoint.setHost(currentCluster);
            }));

            rabbitMq.setPort(getExposedPort(rabbitMq.getPort(), rabbitMqService));
            rabbitMq.setHost(currentCluster);
            rabbitMq.setUsername(username);

            cradle.setPort(getExposedPort(cradle.getPort(), cassandraService));
            cradle.setHost(currentCluster);

            File generatedConfigsDirFile = configPath.toFile();

            if (generatedConfigsDirFile.mkdir()) {
                LOGGER.info("Directory {} is created", configPath);
            } else {
                LOGGER.info("All boxConf in the '{}' folder are overridden", configPath);
            }

            if (generatedConfigsDirFile.exists()) {
                File grpcFile = grpcPath.toFile();
                File rabbitMqFile = rabbitMqPath.toFile();
                File cradleFile = cradlePath.toFile();
                File mqFile = mqPath.toFile();
                File customFile = customPath.toFile();
                File prometheusFile = prometheusPath.toFile();
                File dictionaryFile = dictionaryPath.toFile();

                writeToJson(grpcFile, grpc);
                writeToJson(rabbitMqFile, rabbitMq);
                writeToJson(cradleFile, cradle);
                writeToJson(mqFile, mq);
                writeToJson(customFile, custom);
                writeToJson(prometheusFile, new PrometheusConfiguration());

                //FIXME: backport "box name" related changes
                writeDictionaries("unknown", configPath, dictionaryPath, dictionaries, configMaps.list());
            }

        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return new CommonFactory(rabbitMqPath, rabbitMqPath, grpcPath, cradlePath, customPath, prometheusPath, dictionaryPath, configPath);
    }

    private static void writeDictionaries(String boxName, Path oldDictionariesDir, Path dictionariesDir, Map<DictionaryType, String> dictionaries, ConfigMapList configMapList) throws IOException {
        for (ConfigMap configMap : configMapList.getItems()) {
            String configMapName = configMap.getMetadata().getName();
            if (configMapName.startsWith(boxName) && configMapName.endsWith("-dictionary")) {
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
                    writeFile(dictionaryTypeDir.resolve(fileName), dictionaryConfigMap.getData().get(fileName));

                    break;
                }
            }
        }
    }

    private static int getExposedPort(int port, Service service) {
        for (ServicePort servicePort : service.getSpec().getPorts()) {
            if (servicePort.getPort() == port) {
                return servicePort.getNodePort();
            }
        }
        throw new IllegalStateException("There is no exposed port for the target port " + port);
    }

    private static void writeFile(Path path, String data) throws IOException {
        try (OutputStream outputStream = Files.newOutputStream(path)) {
            IOUtils.write(data, outputStream, Charsets.UTF_8);
        }
    }

    private static void writeToJson(File file, Object object) throws IOException {
        if (file.createNewFile() || file.exists()) {
            MAPPER.writeValue(file, object);
        }
    }

    private static Path calculatePath(String path, String configsPath) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath) : CONFIG_DEFAULT_PATH);
    }

    private static Path calculatePath(String path, String configsPath, String fileName) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath, fileName) : CONFIG_DEFAULT_PATH.resolve(fileName));
    }
}
