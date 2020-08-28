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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

public class Configuration {

    public static final String ENV_GRPC_HOST = "GRPC_HOST";
    public static final String DEFAULT_GRPC_HOST = "localhost";

    public static String getEnvGRPCHost() {
        return defaultIfNull(getenv(ENV_GRPC_HOST), DEFAULT_GRPC_HOST);
    }

    public static final String ENV_GRPC_PORT = "GRPC_PORT";
    public static final int DEFAULT_GRPC_PORT = 8080;

    public static int getEnvGRPCPort() {
        return toInt(getenv(ENV_GRPC_PORT), DEFAULT_GRPC_PORT);
    }

    public static final ObjectMapper YAML_READER = new ObjectMapper(new YAMLFactory());
    public static final ObjectMapper JSON_READER = new ObjectMapper(new JsonFactory());

    public static Configuration load(InputStream inputStream) throws IOException {
        return YAML_READER.readValue(inputStream, Configuration.class);
    }

    /**
     * gRPC port for TH2 microservice
     */
    private int port = getEnvGRPCPort();

    public int getPort() {
        return port;
    }

    public void setPort(int gRPCPort) {
        this.port = gRPCPort;
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "port=" + port +
                '}';
    }
}
