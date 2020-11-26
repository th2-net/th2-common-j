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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.common.schema.message.MessageRouter;
import org.apache.log4j.Logger;

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

    private static final Logger logger = Logger.getLogger(CommonFactory.class.getName());

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

        try {
            CommandLine cmd = new DefaultParser().parse(options, args);

            String configs = cmd.getOptionValue("configs");

            return new CommonFactory(
                    calculatePath(cmd.getOptionValue("rabbitConfiguration"), configs, RABBIT_MQ_FILE_NAME),
                    calculatePath(cmd.getOptionValue("messageRouterConfiguration"), configs, ROUTER_MQ_FILE_NAME),
                    calculatePath(cmd.getOptionValue("grpcRouterConfiguration"), configs, ROUTER_GRPC_FILE_NAME),
                    calculatePath(cmd.getOptionValue("cradleConfiguration"), configs, CRADLE_FILE_NAME),
                    calculatePath(cmd.getOptionValue("customConfiguration"), configs, CUSTOM_FILE_NAME),
                    calculatePath(cmd.getOptionValue("prometheusConfiguration"), configs, PROMETHEUS_FILE_NAME),
                    calculatePath(cmd.getOptionValue("dictionariesDir"), configs)
            );
        } catch (ParseException e) {
            throw new IllegalArgumentException("Incorrect arguments " + Arrays.toString(args), e);
        }
    }

    public static CommonFactory createFromKubernetes(String namespace, String boxName) {

        Resource<ConfigMap, DoneableConfigMap> boxConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> rabbitMqConfigMapResource;
        Resource<ConfigMap, DoneableConfigMap> cradleConfigMapResource;

        ConfigMap boxConfigMap;
        ConfigMap rabbitMqConfigMap;
        ConfigMap cradleConfigMap;

        Path grpcPath = Path.of(System.getProperty("user.dir") + "/generated_configs/" + ROUTER_GRPC_FILE_NAME);
        Path rabbitMqPath = Path.of(System.getProperty("user.dir") + "/generated_configs/" + RABBIT_MQ_FILE_NAME);
        Path cradlePath = Path.of(System.getProperty("user.dir") + "/generated_configs/" + CRADLE_FILE_NAME);
        Path mqPath = Path.of(System.getProperty("user.dir") + "/generated_configs/" + ROUTER_MQ_FILE_NAME);
        Path customPath = Path.of(System.getProperty("user.dir") + "/generated_configs/" + CUSTOM_FILE_NAME);
        Path prometheusPath = Path.of(System.getProperty("user.dir") + "/generated_configs/" + PROMETHEUS_FILE_NAME);
        Path dictionaryPath = Path.of(System.getProperty("user.dir") + "/generated_configs/dictionary.json");

        try(KubernetesClient client = new DefaultKubernetesClient()) {

            String currentCluster = client.getConfiguration().getCurrentContext().getContext().getCluster();

            boxConfigMapResource = client.configMaps().inNamespace(namespace).withName(boxName + "-app-config");
            rabbitMqConfigMapResource = client.configMaps().inNamespace(namespace).withName("rabbit-mq-app-config");
            cradleConfigMapResource = client.configMaps().inNamespace(namespace).withName("cradle");

            boxConfigMap = boxConfigMapResource.require();
            rabbitMqConfigMap = rabbitMqConfigMapResource.require();
            cradleConfigMap = cradleConfigMapResource.require();

            ConfigMap dictionaryConfigMap = getDictionary(boxName, client.configMaps().list());

            Map<String, String> boxData = boxConfigMap.getData();
            Map<String, String> rabbitMqData = rabbitMqConfigMap.getData();
            Map<String, String> cradleConfigData = cradleConfigMap.getData();

            Service rabbitMqService = client.services().inNamespace("service").withName("rabbitmq-schema").require();
            Service cassandraService = client.services().inNamespace("service").withName("cassandra-schema").require();

            ObjectNode grpcNode = new ObjectMapper().readValue(boxData.get("grpc.json"), ObjectNode.class);
            ObjectNode mqNode = new ObjectMapper().readValue(boxData.get("mq.json"), ObjectNode.class);
            ObjectNode customNode = new ObjectMapper().readValue(boxData.get("custom.json"), ObjectNode.class);

            ObjectNode rabbitMqNode = new ObjectMapper().readValue(rabbitMqData.get("rabbitMQ.json"), ObjectNode.class);
            ObjectNode cradleNode = new ObjectMapper().readValue(cradleConfigData.get("cradle.json"), ObjectNode.class);

            if(grpcNode.has("services")) {
                ObjectNode serverNode = (ObjectNode) grpcNode.get("services");

                for (Iterator<String> service = serverNode.fieldNames(); service.hasNext(); ) {
                    String serviceName = service.next();

                    JsonNode serviceNode = serverNode.get(serviceName);

                    JsonNode endpointsNode = serviceNode.get("endpoints");

                    for (Iterator<String> endpoint = endpointsNode.fieldNames(); endpoint.hasNext(); ) {
                        String endpointName = endpoint.next();

                        ObjectNode endpointNode = (ObjectNode) endpointsNode.get(endpointName);

                        Service boxService = client.services().inNamespace(namespace).withName(endpointNode.get("host").asText()).require();

                        changePort(endpointNode, boxService);

                        endpointNode.put("host", currentCluster);

                    }
                }
            }

            if(rabbitMqNode.has("port")) {
                changePort(rabbitMqNode, rabbitMqService);
            }

            if(rabbitMqNode.has("host")) {
                rabbitMqNode.put("host", currentCluster);
            }

            if(cradleNode.has("port")) {
                changePort(cradleNode, cassandraService);
            }

            if(cradleNode.has("host")) {
                cradleNode.put("host", currentCluster);
            }

            File generatedConfigsDir = new File(System.getProperty("user.dir") + "/generated_configs/");

            if(generatedConfigsDir.mkdir()) {
                logger.info("Directory \"generated_configs\" is created at " + System.getProperty("user.dir"));
            }

            if(generatedConfigsDir.exists()) {
                File grpcFile = new File(String.valueOf(grpcPath));
                File rabbitMqFile = new File(String.valueOf(rabbitMqPath));
                File cradleFile = new File(String.valueOf(cradlePath));
                File mqFile = new File(String.valueOf(mqPath));
                File customFile = new File(String.valueOf(customPath));
                File prometheusFile = new File(String.valueOf(prometheusPath));
                File dictionaryFile = new File(String.valueOf(dictionaryPath));

                writeToJson(grpcFile, String.valueOf(grpcNode));
                writeToJson(rabbitMqFile, String.valueOf(rabbitMqNode));
                writeToJson(cradleFile, String.valueOf(cradleNode));
                writeToJson(mqFile, String.valueOf(mqNode));
                writeToJson(customFile, String.valueOf(customNode));

                if(prometheusFile.createNewFile() || prometheusFile.exists()) {
                    try(Writer prometheusWriter = new FileWriter(prometheusFile)) {
                        prometheusWriter.write("{\"host\":\"\",\"port\":null,\"enabled\":false}");
                    }
                }

                if(dictionaryFile.createNewFile() || dictionaryFile.exists()) {
                    try(Writer dictionaryWriter = new FileWriter(dictionaryFile)) {
                        if(dictionaryConfigMap != null) {
                            dictionaryWriter.write(dictionaryConfigMap.getData().toString());
                        }
                    }
                }
            }

        } catch (IOException e) {
            logger.error(e);
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

    private static void changePort(ObjectNode node, Service service) {
        for(int i = 0; i < service.getSpec().getPorts().size(); i++) {
            if(service.getSpec().getPorts().get(i).getPort() == node.get("port").asInt()) {
                node.put("port", service.getSpec().getPorts().get(i).getNodePort());
                break;
            }
        }
    }

    private static void writeToJson(File file, String data) throws IOException {
        if(file.createNewFile() || file.exists()) {
            try(Writer customWriter = new FileWriter(file)) {
                customWriter.write(String.valueOf(data));
            }
        }
    }


    private static Path calculatePath(String path, String configsPath) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath) : CONFIG_DEFAULT_PATH);
    }

    private static Path calculatePath(String path, String configsPath, String fileName) {
        return path != null ? Path.of(path) : (configsPath != null ? Path.of(configsPath, fileName) : CONFIG_DEFAULT_PATH.resolve(fileName));
    }
}
