/******************************************************************************
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
 ******************************************************************************/
package com.exactpro.th2.configuration;

import static com.exactpro.th2.configuration.Configuration.JSON_READER;
import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;

public class Th2Configuration {
    private final static Logger LOGGER = LoggerFactory.getLogger(Th2Configuration.class);

    public static final String ENV_RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY = "RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY";
    public static final String DEFAULT_RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY = "RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY";

    public static String getEnvRabbitMQExchangeNameTH2Connectivity() {
        return defaultIfNull(getenv(ENV_RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY), DEFAULT_RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY);
    }

    public static final String ENV_TH2_EVENT_STORAGE_GRPC_HOST = "TH2_EVENT_STORAGE_GRPC_HOST";
    public static final String DEFAULT_TH2_EVENT_STORAGE_GRPC_HOST = "localhost";

    public static String getEnvTH2EventStorageGRPCHost() {
        return defaultIfNull(getenv(ENV_TH2_EVENT_STORAGE_GRPC_HOST), DEFAULT_TH2_EVENT_STORAGE_GRPC_HOST);
    }

    public static final String ENV_TH2_EVENT_STORAGE_GRPC_PORT = "TH2_EVENT_STORAGE_GRPC_PORT";
    public static final int DEFAULT_TH2_EVENT_STORAGE_GRPC_PORT = 8080;

    public static int getEnvTH2EventStorageGRPCPort() {
        return toInt(getenv(ENV_TH2_EVENT_STORAGE_GRPC_PORT), DEFAULT_TH2_EVENT_STORAGE_GRPC_PORT);
    }

    public static final String ENV_TH2_VERIFIER_GRPC_HOST = "TH2_VERIFIER_GRPC_HOST";
    public static final String DEFAULT_TH2_VERIFIER_GRPC_HOST = "localhost";

    public static String getEnvTH2VerifierGRPCHost() {
        return defaultIfNull(getenv(ENV_TH2_VERIFIER_GRPC_HOST), DEFAULT_TH2_VERIFIER_GRPC_HOST);
    }

    public static final String ENV_TH2_VERIFIER_GRPC_PORT = "TH2_VERIFIER_GRPC_PORT";
    public static final int DEFAULT_TH2_VERIFIER_GRPC_PORT = 8080;

    public static int getEnvTH2VerifierGRPCPort() {
        return toInt(getenv(ENV_TH2_VERIFIER_GRPC_PORT), DEFAULT_TH2_VERIFIER_GRPC_PORT);
    }

    /**
     * Format for this parameter: {"connectivity_name": {"exchangeName":"value", "toSendQueueName":"value", "toSendRawQueueName":"value", "inQueueName": "value", "inRawQueueName": "value", "outQueueName": "value" , "outRawQueueName": "value"  }}
     * for example: {"fix_client": {"exchangeName":"demo_exchange", "toSendQueueName":"client_to_send", "toSendRawQueueName":"client_to_send_raw", "inQueueName": "fix_codec_out_client", "inRawQueueName": "client_in_raw", "outQueueName": "client_out" , "outRawQueueName": "client_out_raw"  }, "fix_server": {"exchangeName":"demo_exchange", "toSendQueueName":"server_to_send", "toSendRawQueueName":"server_to_send_raw", "inQueueName": "fix_codec_out_server", "inRawQueueName": "server_in_raw", "outQueueName": "server_out" , "outRawQueueName": "server_out_raw"  }}
     */
    public static final String ENV_TH2_CONNECTIVITY_QUEUE_NAMES = "TH2_CONNECTIVITY_QUEUE_NAMES";
    public static final String DEFAULT_TH2_CONNECTIVITY_QUEUE_NAMES  = "{}";

    public static Map<String, QueueNames> getEnvTh2ConnectivityQueueNames() {
        String connectivityAddressesJSON = defaultIfNull(getenv(ENV_TH2_CONNECTIVITY_QUEUE_NAMES), DEFAULT_TH2_CONNECTIVITY_QUEUE_NAMES);
        try {
            return ImmutableMap.copyOf(JSON_READER.<Map<String, QueueNames>>readValue(connectivityAddressesJSON, new TypeReference<Map<String, QueueNames>>() {}));
        } catch (IOException e) {
            throw new IllegalArgumentException(ENV_TH2_CONNECTIVITY_QUEUE_NAMES + " environment variable value '" + connectivityAddressesJSON + "' can't be read", e);
        }
    }

    public static Th2Configuration load(InputStream inputStream) throws IOException {
        return Configuration.YAML_READER.readValue(inputStream, Th2Configuration.class);
    }

    private Map<String, QueueNames> connectivityQueueNames = getEnvTh2ConnectivityQueueNames();
    private String rabbitMQExchangeNameTH2Connectivity = getEnvRabbitMQExchangeNameTH2Connectivity();
    private String th2EventStorageGRPCHost = getEnvTH2EventStorageGRPCHost();
    private int th2EventStorageGRPCPort = getEnvTH2EventStorageGRPCPort();
    private String th2VerifierGRPCHost = getEnvTH2VerifierGRPCHost();
    private int th2VerifierGRPCPort = getEnvTH2VerifierGRPCPort();

    public String getRabbitMQExchangeNameTH2Connectivity() {
        return rabbitMQExchangeNameTH2Connectivity;
    }

    public void setRabbitMQExchangeNameTH2Connectivity(String rabbitMQExchangeNameTH2Connectivity) {
        this.rabbitMQExchangeNameTH2Connectivity = rabbitMQExchangeNameTH2Connectivity;
    }

    public String getTh2EventStorageGRPCHost() {
        return th2EventStorageGRPCHost;
    }

    public void setTh2EventStorageGRPCHost(String th2EventStorageGRPCHost) {
        this.th2EventStorageGRPCHost = th2EventStorageGRPCHost;
    }

    public int getTh2EventStorageGRPCPort() {
        return th2EventStorageGRPCPort;
    }

    public void setTh2EventStorageGRPCPort(int th2EventStorageGRPCPort) {
        this.th2EventStorageGRPCPort = th2EventStorageGRPCPort;
    }

    public String getTh2VerifierGRPCHost() {
        return th2VerifierGRPCHost;
    }

    public void setTh2VerifierGRPCHost(String th2VerifierGRPCHost) {
        this.th2VerifierGRPCHost = th2VerifierGRPCHost;
    }

    public int getTh2VerifierGRPCPort() {
        return th2VerifierGRPCPort;
    }

    public void setTh2VerifierGRPCPort(int th2VerifierGRPCPort) {
        this.th2VerifierGRPCPort = th2VerifierGRPCPort;
    }

    public Map<String, QueueNames> getConnectivityQueueNames() {
        return connectivityQueueNames;
    }

    public void setConnectivityQueueNames(Map<String, QueueNames> connectivityQueueNames) {
        this.connectivityQueueNames = connectivityQueueNames;
    }

    public static class QueueNames {
        private String exchangeName;
        private String inQueueName;
        private String inRawQueueName;
        private String outQueueName;
        private String outRawQueueName;
        private String toSendQueueName;
        private String toSendRawQueueName;

        public QueueNames() {}

        public QueueNames(String exchangeName,
                          String inQueueName,
                          String inRawQueueName,
                          String outQueueName,
                          String outRawQueueName,
                          String toSendQueueName,
                          String toSendRawQueueName) {
            this.exchangeName = exchangeName;
            this.inQueueName = inQueueName;
            this.inRawQueueName = inRawQueueName;
            this.outQueueName = outQueueName;
            this.outRawQueueName = outRawQueueName;
            this.toSendQueueName = toSendQueueName;
            this.toSendRawQueueName = toSendRawQueueName;
        }

        public String getInQueueName() {
            return inQueueName;
        }

        public void setInQueueName(String inQueueName) {
            this.inQueueName = inQueueName;
        }

        public String getInRawQueueName() {
            return inRawQueueName;
        }

        public void setInRawQueueName(String inRawQueueName) {
            this.inRawQueueName = inRawQueueName;
        }

        public String getOutQueueName() {
            return outQueueName;
        }

        public void setOutQueueName(String outQueueName) {
            this.outQueueName = outQueueName;
        }

        public String getOutRawQueueName() {
            return outRawQueueName;
        }

        public void setOutRawQueueName(String outRawQueueName) {
            this.outRawQueueName = outRawQueueName;
        }

        public String getToSendQueueName() {
            return toSendQueueName;
        }

        public void setToSendQueueName(String toSendQueueName) {
            this.toSendQueueName = toSendQueueName;
        }

        public String getToSendRawQueueName() {
            return toSendRawQueueName;
        }

        public void setToSendRawQueueName(String toSendRawQueueName) {
            this.toSendRawQueueName = toSendRawQueueName;
        }

        public String getExchangeName() {
            return exchangeName;
        }

        public void setExchangeName(String exchangeName) {
            this.exchangeName = exchangeName;
        }

        @Override
        public String toString() {
            return "QueueNames{" +
                    "exchangeName='" + exchangeName + '\'' +
                    ", inQueueName='" + inQueueName + '\'' +
                    ", inRawQueueName='" + inRawQueueName + '\'' +
                    ", outQueueName='" + outQueueName + '\'' +
                    ", outRawQueueName='" + outRawQueueName + '\'' +
                    ", toSendQueueName='" + toSendQueueName + '\'' +
                    ", toSendRawQueueName='" + toSendRawQueueName + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "Th2Configuration{" +
                "connectivityQueueNames=" + connectivityQueueNames +
                ", rabbitMQExchangeNameTH2Connectivity='" + rabbitMQExchangeNameTH2Connectivity + '\'' +
                ", th2EventStorageGRPCHost='" + th2EventStorageGRPCHost + '\'' +
                ", th2EventStorageGRPCPort=" + th2EventStorageGRPCPort +
                ", th2VerifierGRPCHost='" + th2VerifierGRPCHost + '\'' +
                ", th2VerifierGRPCPort=" + th2VerifierGRPCPort +
                '}';
    }
}
