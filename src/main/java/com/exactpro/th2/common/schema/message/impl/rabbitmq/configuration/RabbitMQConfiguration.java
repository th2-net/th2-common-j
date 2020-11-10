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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class RabbitMQConfiguration {

    private String host;

    @JsonProperty
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

    public void setPrefetchCount(int prefetchCount) {
        if (prefetchCount > -1) {
            this.prefetchCount = prefetchCount;
        }
    }

    @Override
    public String toString() {
        return "RabbitMQConfiguration{" +
                "host='" + host + '\'' +
                ", vHost='" + vHost + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", subscriberName='" + subscriberName + '\'' +
                ", exchangeName='" + exchangeName + '\'' +
                ", connectionTimeout=" + connectionTimeout +
                ", connectionCloseTimeout=" + connectionCloseTimeout +
                ", maxRecoveryAttempts=" + maxRecoveryAttempts +
                ", minConnectionRecoveryTimeout=" + minConnectionRecoveryTimeout +
                ", maxConnectionRecoveryTimeout=" + maxConnectionRecoveryTimeout +
                ", prefetchCount=" + prefetchCount +
                '}';
    }
}
