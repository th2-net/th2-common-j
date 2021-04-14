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
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.event.EventBatchRouter;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.grpc.router.impl.DefaultGrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.group.RabbitMessageGroupBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchRouter;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchRouter;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.api.model.Secret;
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
    private static final String DICTIONARY_FILE_NAME = "dictionary.json";
    private static final String BOX_FILE_NAME = "box.json";

    private static final String RABBITMQ_SECRET_NAME = "rabbitmq";
    private static final String CASSANDRA_SECRET_NAME = "cassandra";
    private static final String RABBITMQ_PASSWORD_KEY = "rabbitmq-password";
    private static final String CASSANDRA_PASSWORD_KEY = "cassandra-password";

    private static final String KEY_RABBITMQ_PASS = "RABBITMQ_PASS";
    private static final String KEY_CASSANDRA_PASS = "CASSANDRA_PASS";

    private final Path rabbitMQ;
    private final Path routerMQ;
    private final Path routerGRPC;
    private final Path prometheus;
    private final Path cradle;
    private final Path custom;
    private final Path dictionariesDir;
    private final Path boxConfiguration;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFactory.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    protected CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            Class<? extends GrpcRouter> grpcRouterClass,
            Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration,
            Map<String, String> environmentVariables) {
        super(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass, environmentVariables);
        this.rabbitMQ = rabbitMQ;
        this.routerMQ = routerMQ;
        this.routerGRPC = routerGRPC;
        this.cradle = cradle;
        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
        this.prometheus = prometheus;
        this.boxConfiguration = boxConfiguration;

        start();
    }

    protected CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration, Map<String, String> variables) {
        this(RabbitParsedBatchRouter.class, RabbitRawBatchRouter.class, RabbitMessageGroupBatchRouter.class, EventBatchRouter.class, DefaultGrpcRouter.class,
                rabbitMQ, routerMQ, routerGRPC, cradle, custom, prometheus, dictionariesDir, boxConfiguration, variables);
    }

    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            Class<? extends GrpcRouter> grpcRouterClass,
            Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {
        this(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass,
                rabbitMQ ,routerMQ ,routerGRPC ,cradle ,custom ,dictionariesDir ,prometheus ,boxConfiguration, emptyMap());
    }

    public CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {
        this(RabbitParsedBatchRouter.class, RabbitRawBatchRouter.class, RabbitMessageGroupBatchRouter.class, EventBatchRouter.class, DefaultGrpcRouter.class,
                rabbitMQ ,routerMQ ,routerGRPC ,cradle ,custom ,dictionariesDir ,prometheus ,boxConfiguration);
    }

    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            Class<? extends GrpcRouter> grpcRouterClass) {
        this(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass,
                CONFIG_DEFAULT_PATH.resolve(RABBIT_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_GRPC_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CRADLE_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CUSTOM_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(PROMETHEUS_FILE_NAME),
                CONFIG_DEFAULT_PATH,
                CONFIG_DEFAULT_PATH.resolve(BOX_FILE_NAME)
                );
    }

    public CommonFactory() {
        this(CONFIG_DEFAULT_PATH.resolve(RABBIT_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_MQ_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(ROUTER_GRPC_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CRADLE_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(CUSTOM_FILE_NAME),
                CONFIG_DEFAULT_PATH.resolve(PROMETHEUS_FILE_NAME),
                CONFIG_DEFAULT_PATH,
                CONFIG_DEFAULT_PATH.resolve(BOX_FILE_NAME)
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
    protected Path getPathToPrometheusConfiguration() {
        return prometheus;
    }

    @Override
    protected Path getPathToBoxConfiguration() {
        return boxConfiguration;
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
     *             --boxConfiguration - path to json file with boxes configuration and information
     *             <p>
     *             --namespace - namespace in Kubernetes to find config maps related to the target
     *             <p>
     *             --boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
     *             <p>
     *             --contextName - context name to choose the context from Kube config
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

        Option rabbitConfigurationOption = new Option(null, "rabbitConfiguration", true, null);
        Option messageRouterConfigurationOption = new Option(null, "messageRouterConfiguration", true, null);
        Option grpcRouterConfigurationOption = new Option(null, "grpcRouterConfiguration", true, null);
        Option cradleConfigurationOption = new Option(null, "cradleConfiguration", true, null);
        Option customConfigurationOption = new Option(null, "customConfiguration", true, null);
        Option dictionariesDirOption = new Option(null, "dictionariesDir", true, null);
        Option prometheusConfigurationOption = new Option(null, "prometheusConfiguration", true, null);
        Option boxConfigurationOption = new Option(null, "boxConfiguration", true, null);
        Option configOption = new Option("c", "configs", true, null);
        Option namespaceOption = new Option(null, "namespace", true, null);
        Option boxNameOption = new Option(null, "boxName", true, null);
        Option contextNameOption = new Option(null, "contextName", true, null);

        options.addOption(rabbitConfigurationOption);
        options.addOption(messageRouterConfigurationOption);
        options.addOption(grpcRouterConfigurationOption);
        options.addOption(cradleConfigurationOption);
        options.addOption(customConfigurationOption);
        options.addOption(dictionariesDirOption);
        options.addOption(prometheusConfigurationOption);
        options.addOption(boxConfigurationOption);
        options.addOption(configOption);
        options.addOption(namespaceOption);
        options.addOption(boxNameOption);
        options.addOption(contextNameOption);

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);

            String configs = cmd.getOptionValue(configOption.getLongOpt());

            if (cmd.hasOption(namespaceOption.getLongOpt()) && cmd.hasOption(boxNameOption.getLongOpt())) {
                String namespace = cmd.getOptionValue(namespaceOption.getLongOpt());
                String boxName = cmd.getOptionValue(boxNameOption.getLongOpt());
                String contextName = cmd.getOptionValue(contextNameOption.getLongOpt());

                return createFromKubernetes(namespace, boxName, contextName);
            } else {
                return new CommonFactory(
                        calculatePath(cmd.getOptionValue(rabbitConfigurationOption.getLongOpt()), configs, RABBIT_MQ_FILE_NAME),
                        calculatePath(cmd.getOptionValue(messageRouterConfigurationOption.getLongOpt()), configs, ROUTER_MQ_FILE_NAME),
                        calculatePath(cmd.getOptionValue(grpcRouterConfigurationOption.getLongOpt()), configs, ROUTER_GRPC_FILE_NAME),
                        calculatePath(cmd.getOptionValue(cradleConfigurationOption.getLongOpt()), configs, CRADLE_FILE_NAME),
                        calculatePath(cmd.getOptionValue(customConfigurationOption.getLongOpt()), configs, CUSTOM_FILE_NAME),
                        calculatePath(cmd.getOptionValue(prometheusConfigurationOption.getLongOpt()), configs, PROMETHEUS_FILE_NAME),
                        calculatePath(cmd.getOptionValue(dictionariesDirOption.getLongOpt()), configs),
                        calculatePath(cmd.getOptionValue(boxNameOption.getLongOpt()), configs, BOX_FILE_NAME)
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
        return createFromKubernetes(namespace, boxName, null);
    }

    /**
     * Create {@link CommonFactory} via configs map from Kubernetes
     *
     * @param namespace - namespace in Kubernetes to find config maps related to the target th2 box
     * @param boxName - the name of the target th2 box placed in the specified namespace in Kubernetes
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

        Path grpcPath = Path.of(userDir, generatedConfigsDir, ROUTER_GRPC_FILE_NAME);
        Path rabbitMqPath = Path.of(userDir, generatedConfigsDir, RABBIT_MQ_FILE_NAME);
        Path cradlePath = Path.of(userDir, generatedConfigsDir, CRADLE_FILE_NAME);
        Path mqPath = Path.of(userDir, generatedConfigsDir, ROUTER_MQ_FILE_NAME);
        Path customPath = Path.of(userDir, generatedConfigsDir, CUSTOM_FILE_NAME);
        Path prometheusPath = Path.of(userDir, generatedConfigsDir, PROMETHEUS_FILE_NAME);
        Path dictionaryPath = Path.of(userDir,generatedConfigsDir, DICTIONARY_FILE_NAME);
        Path boxConfigurationPath = Path.of(userDir, generatedConfigsDir, BOX_FILE_NAME);

        try(KubernetesClient client = new DefaultKubernetesClient()) {

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
                    throw new IllegalArgumentException("Failed to find context "+contextName);
            }

            Secret rabbitMqSecret = requireNonNull(client.secrets().inNamespace(namespace).withName(RABBITMQ_SECRET_NAME).get(),
                    "Secret '"+ RABBITMQ_SECRET_NAME +"' isn't found in namespace " + namespace);
            Secret cassandraSecret = requireNonNull(client.secrets().inNamespace(namespace).withName(CASSANDRA_SECRET_NAME).get(),
                    "Secret '"+ CASSANDRA_SECRET_NAME +"' isn't found in namespace " + namespace);

            String encodedRabbitMqPass = requireNonNull(rabbitMqSecret.getData().get(RABBITMQ_PASSWORD_KEY),
                            "Key '" + RABBITMQ_PASSWORD_KEY + "' not found in secret '" + RABBITMQ_SECRET_NAME + "' in namespace " + namespace);
            String encodedCassandraPass = requireNonNull(cassandraSecret.getData().get(CASSANDRA_PASSWORD_KEY),
                            "Key '" + CASSANDRA_PASSWORD_KEY + "' not found in secret '" + CASSANDRA_SECRET_NAME + "' in namespace " + namespace);

            String rabbitMqPassword = new String(Base64.getDecoder().decode(encodedRabbitMqPass));
            String cassandraPassword = new String(Base64.getDecoder().decode(encodedCassandraPass));

            Map<String, String> environmentVariables = new HashMap<>();
            environmentVariables.put(KEY_RABBITMQ_PASS, rabbitMqPassword);
            environmentVariables.put(KEY_CASSANDRA_PASS, cassandraPassword);

            var configMaps= client.configMaps();

            boxConfigMapResource = configMaps.inNamespace(namespace).withName(boxName + "-app-config");
            rabbitMqConfigMapResource = configMaps.inNamespace(namespace).withName("rabbit-mq-external-app-config");
            cradleConfigMapResource = configMaps.inNamespace(namespace).withName("cradle-external");

            if (boxConfigMapResource.get() == null)
                throw new IllegalArgumentException("Failed to find config maps by boxName "+boxName);

            boxConfigMap = boxConfigMapResource.require();
            rabbitMqConfigMap = rabbitMqConfigMapResource.require();
            cradleConfigMap = cradleConfigMapResource.require();

            ConfigMap dictionaryConfigMap = getDictionary(boxName, client.configMaps().list());

            Map<String, String> boxData = boxConfigMap.getData();
            Map<String, String> rabbitMqData = rabbitMqConfigMap.getData();
            Map<String, String> cradleConfigData = cradleConfigMap.getData();

            File generatedConfigsDirFile = new File(userDir, generatedConfigsDir);

            if(generatedConfigsDirFile.mkdir()) {
                LOGGER.info("Directory {} is created at {}", generatedConfigsDir, userDir);
            } else {
                LOGGER.info("All boxConf in the '{}' folder are overridden", generatedConfigsDir);
            }

            if(generatedConfigsDirFile.exists()) {
                BoxConfiguration box = new BoxConfiguration();
                box.setBoxName(boxName);

                writeFile(grpcPath, boxData.get(ROUTER_GRPC_FILE_NAME));
                writeFile(rabbitMqPath, rabbitMqData.get(RABBIT_MQ_FILE_NAME));
                writeFile(cradlePath, cradleConfigData.get(CRADLE_FILE_NAME));
                writeFile(mqPath, boxData.get(ROUTER_MQ_FILE_NAME));
                writeFile(customPath, boxData.get(CUSTOM_FILE_NAME));
                writeFile(prometheusPath, boxData.get(PROMETHEUS_FILE_NAME));

                String boxConfig = boxData.get(BOX_FILE_NAME);

                if (boxConfig != null)
                    writeFile(boxConfigurationPath, boxConfig);
                else
                    writeToJson(boxConfigurationPath, box);

                if(dictionaryConfigMap != null) {
                    writeToJson(dictionaryPath, dictionaryConfigMap.getData());
                }
            }

            return new CommonFactory(rabbitMqPath, mqPath, grpcPath, cradlePath, customPath, prometheusPath, dictionaryPath, boxConfigurationPath, environmentVariables);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private static ConfigMap getDictionary(String boxName, ConfigMapList configMapList) {
        for(ConfigMap c : configMapList.getItems()) {
            if(c.getMetadata().getName().startsWith(boxName) && c.getMetadata().getName().endsWith("-dictionary")) {
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
        if(file.createNewFile() || file.exists()) {
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
