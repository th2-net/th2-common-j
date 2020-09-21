/*****************************************************************************
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
 *****************************************************************************/

package com.exactpro.th2.schema.message.impl.rabbitmq.configuration;

public class RabbitMQConfiguration {

    private String host;
    private String vHost;
    private int port;
    private String username;
    private String password;
    private String subscriberName;

    public String getHost() {
        return host;
    }

    public String getvHost() {
        return vHost;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSubscriberName() {
        return subscriberName;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setvHost(String vHost) {
        this.vHost = vHost;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSubscriberName(String subscriberName) {
        this.subscriberName = subscriberName;
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
                '}';
    }
}
