/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.cradle

import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.th2.common.schema.configuration.Configuration
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

data class CradleConfidentialConfiguration(
    @JsonProperty(required = true) var dataCenter: String,
    @JsonProperty(required = true) var host: String,
    @JsonProperty(required = true) var keyspace: String,
    var port: Int = 0,
    var username: String? = null,
    var password: String? = null
) : Configuration()

@JsonIgnoreProperties(ignoreUnknown = true)
data class CradleNonConfidentialConfiguration(
    var prepareStorage: Boolean = false,
    @Deprecated("Please use CassandraStorageSettings.resultPageSize")
    var pageSize: Int = 5000,
    var cradleMaxEventBatchSize: Long = CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE.toLong(),
    @Deprecated("Please use CassandraStorageSettings.maxMessageBatchSize")
    var cradleMaxMessageBatchSize: Long = CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE.toLong(),
    @Deprecated("Please use CassandraStorageSettings.counterPersistenceInterval")
    var statisticsPersistenceIntervalMillis: Long = CassandraStorageSettings.DEFAULT_COUNTER_PERSISTENCE_INTERVAL_MS.toLong(),
    @Deprecated("Please use CassandraStorageSettings.maxUncompressedTestEventSize")
    var maxUncompressedEventBatchSize: Long = CassandraStorageSettings.DEFAULT_MAX_UNCOMPRESSED_TEST_EVENT_SIZE.toLong(),
) : Configuration()