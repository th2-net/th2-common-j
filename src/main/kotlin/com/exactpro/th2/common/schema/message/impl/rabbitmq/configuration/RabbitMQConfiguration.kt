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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty

@JsonIgnoreProperties(ignoreUnknown = true)
data class RabbitMQConfiguration(
    var host: String? = null,
    @JsonProperty("vHost") var vHost: String? = null,
    var port: Int = 0,
    var username: String? = null,
    var password: String? = null,
    var subscriberName: String? = null,
    var exchangeName: String? = null) {}

@JsonIgnoreProperties(ignoreUnknown = true)
data class ConnectionManagerConfiguration(
    var connectionTimeout: Int = -1,
    var connectionCloseTimeout: Int = 10000,
    var maxRecoveryAttempts: Int = 5,
    var minConnectionRecoveryTimeout: Int = 10000,
    var maxConnectionRecoveryTimeout: Int = 60000,
    val prefetchCount: Int = 10)