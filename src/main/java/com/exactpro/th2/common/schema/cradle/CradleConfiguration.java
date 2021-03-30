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

package com.exactpro.th2.common.schema.cradle;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraStorageSettings;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CradleConfiguration {

    @JsonProperty(required = true)
    private String dataCenter;

    @JsonProperty(required = true)
    private String host;

    @JsonProperty
    private int port;

    @JsonProperty(required = true)
    private String keyspace;

    @JsonProperty
    private String username;

    @JsonProperty
    private String password;

    @JsonProperty
    private String cradleInstanceName;

    /**
     * This is the connection timeout {@link CassandraStorageSettings#DEFAULT_TIMEOUT} set as the default value.
     */
    @JsonProperty
    private long timeout = CassandraStorageSettings.DEFAULT_TIMEOUT;

    public String getDataCenter() {
        return dataCenter;
    }

    public void setDataCenter(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
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

    public String getCradleInstanceName() {
        return cradleInstanceName;
    }

    public void setCradleInstanceName(String cradleInstanceName) {
        this.cradleInstanceName = cradleInstanceName;
    }

    public long getTimeout()
    {
        return timeout;
    }

    public void setTimeout(long timeout)
    {
        this.timeout = timeout;
    }
}
