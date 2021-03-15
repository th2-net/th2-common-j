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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RabbitMQConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConfiguration.class);

    private String host;
    @JsonProperty
    @JsonAlias("vHost")
    private String vHost;
    private int port;
    private String username;
    private String password;
    private String subscriberName;
    private String exchangeName;

    private int connectionTimeout = -1;
    private int connectionCloseTimeout = 10_000;
    private int maxRecoveryAttempts = 5;
    private int minConnectionRecoveryTimeout = 10_000;
    private int maxConnectionRecoveryTimeout = 60_000;
    private int prefetchCount = 10;

    @JsonProperty("resendMessage")
    private ResendMessageConfiguration resendMessageConfiguration = new ResendMessageConfiguration();

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getVhost() {
        return vHost;
    }

    public void setVhost(String vhost) {
        this.vHost = vhost;
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

    public String getSubscriberName() {
        return subscriberName;
    }

    public void setSubscriberName(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getConnectionCloseTimeout() {
        return connectionCloseTimeout;
    }

    public void setConnectionCloseTimeout(int connectionCloseTimeout) {
        this.connectionCloseTimeout = connectionCloseTimeout;
    }

    public int getMaxRecoveryAttempts() {
        return maxRecoveryAttempts;
    }

    public void setMaxRecoveryAttempts(int maxRecoveryAttempts) {
        this.maxRecoveryAttempts = maxRecoveryAttempts;
    }

    public int getMinConnectionRecoveryTimeout() {
        return minConnectionRecoveryTimeout;
    }

    public void setMinConnectionRecoveryTimeout(int minConnectionRecoveryTimeout) {
        this.minConnectionRecoveryTimeout = minConnectionRecoveryTimeout;
    }

    public int getMaxConnectionRecoveryTimeout() {
        return maxConnectionRecoveryTimeout;
    }

    public void setMaxConnectionRecoveryTimeout(int maxConnectionRecoveryTimeout) {
        this.maxConnectionRecoveryTimeout = maxConnectionRecoveryTimeout;
    }

    public int getPrefetchCount() {
        return prefetchCount;
    }

    public ResendMessageConfiguration getResendMessageConfiguration() {
        return resendMessageConfiguration;
    }

    public void setResendMessageConfiguration(ResendMessageConfiguration resendMessageConfiguration) {
        this.resendMessageConfiguration = resendMessageConfiguration;
    }

    public void setPrefetchCount(int prefetchCount) {
        if (prefetchCount > -1) {
            this.prefetchCount = prefetchCount;
        } else {
            LOGGER.warn("Can not set property 'prefetchCount', because it is must be more than 1, but current value = {}", prefetchCount);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("host", host)
                .append("vhost", vHost)
                .append("port", port)
                .append("username", username)
                .append("password", password)
                .append("subscriberName", subscriberName)
                .append("exchangeName", exchangeName)
                .append("connectionTimeout", connectionTimeout)
                .append("connectionCloseTimeout", connectionCloseTimeout)
                .append("maxRecoveryAttempts", maxRecoveryAttempts)
                .append("minConnectionRecoveryTimeout", minConnectionRecoveryTimeout)
                .append("maxConnectionRecoveryTimeout", maxConnectionRecoveryTimeout)
                .append("prefetchCount", prefetchCount)
                .append("resendMessageConfiguration", resendMessageConfiguration)
                .toString();
    }

}
