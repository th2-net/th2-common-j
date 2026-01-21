/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.configuration

import com.exactpro.th2.common.schema.configuration.Configuration
import com.exactpro.th2.common.util.MultiMapFiltersDeserializer
import com.exactpro.th2.common.util.emptyMultiMap
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.commons.collections4.MultiMapUtils
import org.apache.commons.collections4.MultiValuedMap

data class MessageRouterConfiguration(
    var queues: Map<String, QueueConfiguration> = emptyMap(),
    var globalNotification: GlobalNotificationConfiguration = GlobalNotificationConfiguration()
) : Configuration() {

    fun getQueueByAlias(queueAlias: String): QueueConfiguration? {
        return queues[queueAlias]
    }

    fun findQueuesByAttr(attr: Collection<String>): Map<String, QueueConfiguration> =
        queues.filter { it.value.attributes.containsAll(attr) }
}

data class QueueConfiguration(
    @JsonProperty(required = true) @JsonAlias("name", "sendKey") var routingKey: String,
    @JsonProperty(required = true) @JsonAlias("subscribeKey") var queue: String,
    @JsonProperty(required = true) var exchange: String,
    @JsonProperty(required = true) @JsonAlias("labels", "tags") var attributes: List<String> = emptyList(),
    var filters: List<MqRouterFilterConfiguration> = emptyList(),
    @JsonProperty(value = "read") var isReadable: Boolean = true,
    @JsonProperty(value = "write") var isWritable: Boolean = true
) : Configuration()

data class MqRouterFilterConfiguration(
    @JsonDeserialize(using = MultiMapFiltersDeserializer::class) override var metadata: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap(),
    @JsonDeserialize(using = MultiMapFiltersDeserializer::class) override var message: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap()
) : Configuration(), RouterFilter {

    constructor(
        metadata: Collection<FieldFilterConfiguration> = emptyList(),
        message: Collection<FieldFilterConfiguration> = emptyList()
    ) : this(
        MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().also { metadataMultiMap ->
            metadata.forEach {
                metadataMultiMap.put(it.fieldName, it)
            }
        },
        MultiMapUtils.newListValuedHashMap<String, FieldFilterConfiguration>().also { messageMultiMap ->
            message.forEach {
                messageMultiMap.put(it.fieldName, it)
            }
        }
    )

    @JsonGetter("metadata")
    fun getJsonMetadata(): Collection<FieldFilterConfiguration> = metadata.values()

    @JsonGetter("message")
    fun getJsonMessage(): Collection<FieldFilterConfiguration> = message.values()
}

data class FieldFilterConfigurationOld(
    var value: String? = null,
    @JsonProperty(required = true) var operation: FieldFilterOperation
) : Configuration()

data class FieldFilterConfiguration(
    @JsonProperty(value = "fieldName", required = true) @JsonAlias("fieldName", "field-name") var fieldName: String,
    @JsonProperty("expectedValue") @JsonAlias("value", "expected-value") var expectedValue: String?,
    @JsonProperty(required = true) var operation: FieldFilterOperation
) : Configuration()

enum class FieldFilterOperation {
    EQUAL,
    NOT_EQUAL,
    EMPTY,
    NOT_EMPTY,
    WILDCARD,
    NOT_WILDCARD,
}

data class GlobalNotificationConfiguration(
    @JsonProperty var exchange: String = "global-notification"
) : Configuration()