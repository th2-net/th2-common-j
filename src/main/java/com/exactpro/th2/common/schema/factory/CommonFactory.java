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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.metrics.PrometheusConfiguration;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRouterConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;

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

    private final Path rabbitMQ;
    private final Path routerMQ;
    private final Path routerGRPC;
    private final Path prometheus;
    private final Path cradle;
    private final Path custom;
    private final Path dictionariesDir;

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonFactory.class.getName());
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public CommonFactory(Class<? extends MessageRouter<MessageBatch>> messageRouterParsedBatchClass,
                         Class<? extends MessageRouter<RawMessageBatch>> messageRouterRawBatchClass,
                         Class<? extends MessageRouter<EventBatch>> eventBatchRouterClass,
                         Class<? extends GrpcRouter> grpcRouterClass,
                         Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir) {
        super(messageRouterParsedBatchClass, messageRouterRawBatchClass, eventBatchRouterClass, grpcRouterClass);
        this.rabbitMQ = rabbitMQ;
        this.routerMQ = routerMQ;
        this.routerGRPC = routerGRPC;
        this.cradle = cradle;
        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
        this.prometheus = prometheus;
    }

    public CommonFactory(Path rabbitMQ, Path routerMQ, Path routerGRPC, Path cradle, Path custom, Path prometheus, Path dictionariesDir) {
        this.rabbitMQ = rabbitMQ;
        this.routerMQ = routerMQ;
        this.routerGRPC = routerGRPC;
        this.cradle = cradle;
        this.custom = custom;
        this.dictionariesDir = dictionariesDir;
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

        options.addOption(new Option(null, "rabbitConfiguration", true, null));
        options.addOption(new Option(null, "messageRouterConfiguration", true, null));
        options.addOption(new Option(null, "grpcRouterConfiguration", true, null));
        options.addOption(new Option(null, "cradleConfiguration", true, null));
        options.addOption(new Option(null, "customConfiguration", true, null));
        options.addOption(new Option(null, "dictionariesDir", true, null));
        options.addOption(new Option(null, "prometheusConfiguration", true, null));
        options.addOption(new Option("c", "configs", true, null));

        options.addOption(new Option(null, "namespace", true, null));
        options.addOption(new Option(null, "boxName", true, null));

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);

            String configs = cmd.getOptionValue("configs");

            if(cmd.hasOption("namespace") && cmd.hasOption("boxName")) {
                String namespace = cmd.getOptionValue("namespace");
                String boxName = cmd.getOptionValue("boxName");

                return createFromKubernetes(namespace, boxName);
            } else {
                return new CommonFactory(
                        calculatePath(cmd.getOptionValue("rabbitConfiguration"), configs, RABBIT_MQ_FILE_NAME),
                        calculatePath(cmd.getOptionValue("messageRouterConfiguration"), configs, ROUTER_MQ_FILE_NAME),
                        calculatePath(cmd.getOptionValue("grpcRouterConfiguration"), configs, ROUTER_GRPC_FILE_NAME),
                        calculatePath(cmd.getOptionValue("cradleConfiguration"), configs, CRADLE_FILE_NAME),
                        calculatePath(cmd.getOptionValue("customConfiguration"), configs, CUSTOM_FILE_NAME),
                        calculatePath(cmd.getOptionValue("prometheusConfiguration"), configs, PROMETHEUS_FILE_NAME),
                        calculatePath(cmd.getOptionValue("dictionariesDir"), configs)
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

        try(KubernetesClient client = new DefaultKubernetesClient()) {

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

            ConfigMap dictionaryConfigMap = getDictionary(boxName, client.configMaps().list());

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

            File generatedConfigsDirFile = new File(userDir, generatedConfigsDir);

            if(generatedConfigsDirFile.mkdir()) {
                LOGGER.info("Directory {} is created at {}", generatedConfigsDir, userDir);
            } else {
                LOGGER.info("All configuration in the '{}' folder are overridden", generatedConfigsDir);
            }

            if(generatedConfigsDirFile.exists()) {
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

                if(dictionaryConfigMap != null) {
                    writeToJson(dictionaryFile, dictionaryConfigMap.getData());
                }
            }

        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return new CommonFactory(rabbitMqPath, rabbitMqPath, grpcPath, cradlePath, customPath, prometheusPath, dictionaryPath);
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
