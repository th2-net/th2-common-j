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

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

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
    private static final String BOX_FILE_NAME = "box.json";
    public static final String RABBITMQ_SECRET_NAME = "rabbitmq-schema";
    public static final String CASSANDRA_SECRET_NAME = "cassandra-schema";
    public static final String RABBITMQ_PASSWORD_KEY = "rabbitmq-password";
    public static final String CASSANDRA_PASSWORD_KEY = "cassandra-password";

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

    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
            Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
            Class<? extends MessageRouter<MessageGroupBatch>> messageRouterMessageGroupBatchClass,
            Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
            Class<? extends GrpcRouter> grpcRouterClass,
            Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {
        super(messageRouterParsedBatchClass, messageRouterRawBatchClass, messageRouterMessageGroupBatchClass, eventBatchRouterClass, grpcRouterClass);
        this.rabbitMQ = rabbitMQ;
        this.routerMQ = routerMQ;
        this.routerGRPC = routerGRPC;
        this.cradle = cradle;
        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
        this.prometheus = prometheus;
        this.boxConfiguration = boxConfiguration;
    }

    public CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration) {
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

    protected CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir, Path boxConfiguration, Map<String, String> variables) {
        this(rabbitMQ, routerMQ, routerGRPC, cradle, custom, prometheus, dictionariesDir, boxConfiguration);
        DATA_FROM_SECRETS.putAll(variables);
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

                return createFromKubernetes(namespace, boxName, null);
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
        KubernetesClient client = new DefaultKubernetesClient();
        String currentContextName = client.getConfiguration().getCurrentContext().getName();

        return createFromKubernetes(namespace, boxName, currentContextName);
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
        Path dictionaryPath = Path.of(userDir,generatedConfigsDir, "dictionary.json");
        Path boxConfiguration = Path.of(userDir, generatedConfigsDir, BOX_FILE_NAME);

        try(KubernetesClient client = new DefaultKubernetesClient()) {

            if (contextName != null) {
                for (NamedContext context : client.getConfiguration().getContexts()) {
                    if (context.getName().equals(contextName)) {
                        client.getConfiguration().setCurrentContext(context);
                        break;
                    }
                }
            }

            Secret rabbitMqSecret = client.secrets().inNamespace(namespace).withName(RABBITMQ_SECRET_NAME).get();
            Secret cassandraSecret = client.secrets().inNamespace(namespace).withName(CASSANDRA_SECRET_NAME).get();

            String encodedRabbitMqPass;
            String encodedCassandraPass;

            if (rabbitMqSecret != null && cassandraSecret != null) {
                encodedRabbitMqPass = rabbitMqSecret.getData().get(RABBITMQ_PASSWORD_KEY);
                encodedCassandraPass = cassandraSecret.getData().get(CASSANDRA_PASSWORD_KEY);
            } else {
                LOGGER.error("Failed to find secrets");
                return null;
            }

            String rabbitMqPassword = new String(Base64.getDecoder().decode(encodedRabbitMqPass));
            String cassandraPassword = new String(Base64.getDecoder().decode(encodedCassandraPass));

            DATA_FROM_SECRETS.put("RABBITMQ_PASS", rabbitMqPassword);
            DATA_FROM_SECRETS.put("CASSANDRA_PASS", cassandraPassword);

            var configMaps
                    = client.configMaps();

            boxConfigMapResource = configMaps.inNamespace(namespace).withName(boxName + "-app-config");
            rabbitMqConfigMapResource = configMaps.inNamespace(namespace).withName("rabbit-mq-external-app-config");
            cradleConfigMapResource = configMaps.inNamespace(namespace).withName("cradle-external");

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
                File grpcFile = grpcPath.toFile();
                File rabbitMqFile = rabbitMqPath.toFile();
                File cradleFile = cradlePath.toFile();
                File mqFile = mqPath.toFile();
                File customFile = customPath.toFile();
                File prometheusFile = prometheusPath.toFile();
                File dictionaryFile = dictionaryPath.toFile();
                File boxFile = boxConfiguration.toFile();

                BoxConfiguration box = new BoxConfiguration();
                box.setBoxName(boxName);

                OutputStream grpcOutputStream = new FileOutputStream(grpcFile);
                OutputStream rabbitMqOutputStream = new FileOutputStream(rabbitMqFile);
                OutputStream cradleOutputStream = new FileOutputStream(cradleFile);
                OutputStream mqOutputStream = new FileOutputStream(mqFile);
                OutputStream customOutputStream = new FileOutputStream(customFile);

                IOUtils.write(boxData.get(ROUTER_GRPC_FILE_NAME), grpcOutputStream, Charsets.UTF_8);
                IOUtils.write(rabbitMqData.get(RABBIT_MQ_FILE_NAME), rabbitMqOutputStream, Charsets.UTF_8);
                IOUtils.write(cradleConfigData.get(CRADLE_FILE_NAME), cradleOutputStream, Charsets.UTF_8);
                IOUtils.write(boxData.get(ROUTER_MQ_FILE_NAME), mqOutputStream, Charsets.UTF_8);
                IOUtils.write(boxData.get(CUSTOM_FILE_NAME), customOutputStream, Charsets.UTF_8);

                writeToJson(prometheusFile, new PrometheusConfiguration());
                writeToJson(boxFile, box);

                if(dictionaryConfigMap != null) {
                    writeToJson(dictionaryFile, dictionaryConfigMap.getData());
                }

                if(dictionaryConfigMap != null) {
                    writeToJson(dictionaryFile, dictionaryConfigMap.getData());
                }
            }

            return new CommonFactory(rabbitMqPath, mqPath, grpcPath, cradlePath, customPath, prometheusPath, dictionaryPath, boxConfiguration, DATA_FROM_SECRETS);
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

    private static int getExposedPort(int port, Service service) {
        for(ServicePort servicePort : service.getSpec().getPorts()) {
            if(servicePort.getPort() == port) {
                return servicePort.getNodePort();
            }
        }
        throw new IllegalStateException("There is no exposed port for the target port " + port);
    }

    private static void writeToJson(File file, Object object) throws IOException {
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
