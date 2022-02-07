/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration

import com.exactpro.th2.common.schema.configuration.Configuration
import com.fasterxml.jackson.annotation.JsonProperty

data class RabbitMQConfiguration(
    @JsonProperty(required = true) var host: String,
    @JsonProperty(required = true) @get:JsonProperty("vHost") var vHost: String,
    @JsonProperty(required = true) var port: Int = 5672,
    @JsonProperty(required = true) var username: String,
    @JsonProperty(required = true) var password: String,
    @Deprecated(message = "Please use subscriber name from ConnectionManagerConfiguration")
    var subscriberName: String? = null,  //FIXME: Remove in future version
    var exchangeName: String? = null,
) : Configuration()

data class ConnectionManagerConfiguration(
    var subscriberName: String? = null,
    var connectionTimeout: Int = -1,
    var connectionCloseTimeout: Int = 10000,
    var maxRecoveryAttempts: Int = 5,
    var minConnectionRecoveryTimeout: Int = 10000,
    var maxConnectionRecoveryTimeout: Int = 60000,
    val prefetchCount: Int = 10,
    val messageRecursionLimit: Int = 100,
    val secondsToCheckVirtualQueueLimit: Int = 10,
    val batchesToCheckVirtualQueueLimit: Int = 10000,
) : Configuration()