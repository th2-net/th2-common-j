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
package com.exactpro.th2.common.message.impl.rabbitmq.configuration.impl;

import static com.exactpro.th2.ConfigurationUtils.getEnv;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_AMQP_PORT;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_HOST;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_PASS;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_USER;
import static com.rabbitmq.client.ConnectionFactory.DEFAULT_VHOST;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

import java.io.IOException;
import java.io.InputStream;

import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.ConfigurationUtils;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;

public class DefaultRabbitMQConfiguration implements RabbitMQConfiguration {

    private String host = getEnv("RABBITMQ_HOST", DEFAULT_HOST);
    private String vHost = getEnv("RABBITMQ_VHOST", DEFAULT_VHOST);
    private int port = toInt(getEnv("RABBITMQ_PORT", null), DEFAULT_AMQP_PORT);
    private String username = getEnv("RABBITMQ_USER", DEFAULT_USER);
    private String password = getEnv("RABBITMQ_PASS", DEFAULT_PASS);
    //private String exchangeName = getEnv("RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY", "RABBITMQ_EXCHANGE_NAME_TH2_CONNECTIVITY");
    private String subscriberName = getEnv("RABBITMQ_SUBSCRIBER_NAME", null);


    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getVirtualHost() {
        return vHost;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Nullable
    @Override
    public String getSubscriberName() {
        return subscriberName;
    }

    public static DefaultRabbitMQConfiguration load(InputStream inputStream) throws IOException {
        return ConfigurationUtils.load(DefaultRabbitMQConfiguration.class, inputStream);
    }
}
