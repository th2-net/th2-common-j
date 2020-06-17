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

import static com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;
import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

import java.io.IOException;
import java.io.InputStream;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.StringUtils;

public class RabbitMQConfiguration {
    public static final String ENV_RABBITMQ_HOST = "RABBITMQ_HOST";

    public static String getEnvRabbitMQHost() {
        return defaultIfNull(getenv(ENV_RABBITMQ_HOST), DEFAULT_HOST);
    }

    public static final String ENV_RABBITMQ_PORT = "RABBITMQ_PORT";

    public static int getEnvRabbitMQPort() {
        return toInt(getenv(ENV_RABBITMQ_PORT), DEFAULT_AMQP_PORT);
    }

    public static final String ENV_RABBITMQ_USER = "RABBITMQ_USER";

    public static String getEnvRabbitMQUser() {
        return defaultIfNull(getenv(ENV_RABBITMQ_USER), DEFAULT_USER);
    }

    public static final String ENV_RABBITMQ_PASS = "RABBITMQ_PASS";

    public static String getEnvRabbitMQPass() {
        return defaultIfNull(getenv(ENV_RABBITMQ_PASS), ConnectionFactory.DEFAULT_PASS);
    }

    public static final String ENV_RABBITMQ_VHOST = "RABBITMQ_VHOST";

    public static String getEnvRabbitMQVhost() {
        return defaultIfNull(getenv(ENV_RABBITMQ_VHOST), ConnectionFactory.DEFAULT_VHOST);
    }

    public static RabbitMQConfiguration load(InputStream inputStream) throws IOException {
        return Configuration.YAML_READER.readValue(inputStream, RabbitMQConfiguration.class);
    }

    private  static final int PASSWORD_DEFAULT_LENGTH = 6;

    private String host = getEnvRabbitMQHost();
    private String virtualHost = getEnvRabbitMQVhost();
    private int port = getEnvRabbitMQPort();
    private String username = getEnvRabbitMQUser();
    private String password = getEnvRabbitMQPass();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        int passwordLength = password == null ? PASSWORD_DEFAULT_LENGTH : password.length();
        return "RabbitMQConfiguration{" +
                "host='" + host + '\'' +
                ", virtualHost='" + virtualHost + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + repeat('*', passwordLength) + '\'' +
                '}';
    }
}
