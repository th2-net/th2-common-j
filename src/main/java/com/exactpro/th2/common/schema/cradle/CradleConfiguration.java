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
    
    @JsonProperty
    private long timeout;
    
    @JsonProperty
    private int pageSize;

    /**
     * TThis is the maximum event batch size in bytes with {@link CassandraStorageSettings#DEFAULT_MAX_EVENT_BATCH_SIZE} set as the default value.
     * This option is used as an argument of the {@link CradleManager#initStart(String, boolean, long, long)} method
     */
    private long cradleMaxEventBatchSize = CassandraStorageSettings.DEFAULT_MAX_EVENT_BATCH_SIZE;

    /**
     * This is the maximum message batch size in bytes with {@link CassandraStorageSettings#DEFAULT_MAX_MESSAGE_BATCH_SIZE} set as the default value.
     * This option is used as an argument of the {@link CradleManager#initStart(String, boolean, long, long)} method
     */
    private long cradleMaxMessageBatchSize = CassandraStorageSettings.DEFAULT_MAX_MESSAGE_BATCH_SIZE;

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

    /**
     * Gets the maximum event batch size in bytes.
     */
    public long getCradleMaxEventBatchSize() {
        return cradleMaxEventBatchSize;
    }

    /**
     * Sets the maximum event batch size in bytes.
     */
    public void setCradleMaxEventBatchSize(long cradleMaxEventBatchSize) {
        this.cradleMaxEventBatchSize = cradleMaxEventBatchSize;
    }

    /**
     * Gets the maximum message batch size in bytes.
     */
    public long getCradleMaxMessageBatchSize() {
        return cradleMaxMessageBatchSize;
    }

    /**
     * Sets the maximum message batch size in bytes.
     */
    public void setCradleMaxMessageBatchSize(long cradleMaxMessageBatchSize) {
        this.cradleMaxMessageBatchSize = cradleMaxMessageBatchSize;
    }

    public long getTimeout()
    {
        return timeout;
    }

    public void setTimeout(long timeout)
    {
        this.timeout = timeout;
    }

    public int getPageSize()
    {
        return pageSize;
    }

    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
    }
}
