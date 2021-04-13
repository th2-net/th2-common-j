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

package com.exactpro.th2.common.schema.cradle

import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.fasterxml.jackson.annotation.JsonProperty

data class CradleConfidentialConfiguration(@JsonProperty(required = true) var dataCenter: String,
                                           @JsonProperty(required = true) var host: String,
                                           @JsonProperty(required = true) var keyspace: String,
                                           var port: Int = 0,
                                           var username: String? = null,
                                           var password: String? = null,
                                           var cradleInstanceName: String? = null)

data class CradleNonConfidentialConfiguration(var timeout: Long = CassandraStorageSettings.DEFAULT_TIMEOUT,
                                              var pageSize: Int = 5000,
                                              var cradleMaxEventBatchSize: Long = CassandraStorageSettings.DEFAULT_MAX_EVENT_BATCH_SIZE,
                                              var cradleMaxMessageBatchSize: Long = CassandraStorageSettings.DEFAULT_MAX_MESSAGE_BATCH_SIZE)