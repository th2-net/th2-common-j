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

package com.exactpro.th2.common.schema.message.configuration

import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.schema.configuration.Configuration
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonProperty

data class MessageRouterConfiguration(var queues: Map<String, QueueConfiguration> = emptyMap()) : Configuration() {

    fun getQueueByAlias(queueAlias: String): QueueConfiguration? {
        return queues[queueAlias]
    }

    fun findQueuesByAttr(vararg attrs: String): Map<String, QueueConfiguration> = findQueuesByAttr(attrs.toList())
    fun findQueuesByAttr(attr: Collection<String>): Map<String, QueueConfiguration> =
        queues.filter { it.value.attributes.containsAll(attr) }
}

data class QueueConfiguration(
    @JsonProperty(required = true) @JsonAlias("name", "sendKey") var routingKey: String,
    @JsonProperty(required = true) @JsonAlias("subscribeKey") var queue: String,
    @JsonProperty(required = true) var exchange: String,
    @JsonProperty(required = true) @JsonAlias("labels", "tags") var attributes: List<String> = emptyList(),
    var filters: List<MqRouterFilterConfiguration> = emptyList(),
    @JsonProperty(value = "read", defaultValue = "true") var isReadable: Boolean = true,
    @JsonProperty(value = "write", defaultValue = "true") var isWritable: Boolean = true
) : Configuration()

data class MqRouterFilterConfiguration(
    override var metadata: Map<String, FieldFilterConfiguration> = emptyMap(),
    override var message: Map<String, FieldFilterConfiguration> = emptyMap()
) : Configuration(), RouterFilter

data class FieldFilterConfiguration(
    var value: String? = null,
    @JsonProperty(required = true) var operation: FilterOperation? = null
) : Configuration()