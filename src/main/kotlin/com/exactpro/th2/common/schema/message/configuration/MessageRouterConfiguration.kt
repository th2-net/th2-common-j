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
import com.exactpro.th2.common.util.DeserializerMultiMap
import com.exactpro.th2.common.util.emptyMultiMap
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.commons.collections4.MultiMapUtils
import org.apache.commons.collections4.MultiValuedMap

data class MessageRouterConfiguration(var queues: Map<String, QueueConfiguration> = emptyMap()) : Configuration() {

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
    @JsonProperty var filters: List<MqRouterFilterConfiguration> = emptyList(),
    @JsonProperty(value = "read") var isReadable: Boolean = true,
    @JsonProperty(value = "write") var isWritable: Boolean = true
) : Configuration()

data class MqRouterFilterConfiguration(
    override var metadata: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap(),
    override var message: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap()
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

data class FieldFilterConfiguration(
    @JsonProperty(value = "fieldName", required = true) @JsonAlias("name") var fieldName: String,
    @JsonProperty("expectedValue") @JsonAlias("value") var expectedValue: String,
    @JsonProperty(required = true) var operation: FilterOperation
) : Configuration()

// FieldFilterConfiguration has required field 'fieldName', so we add field to node
// and use default deserializer
val DESERIALIZER_MULTI_MAP_FIELD_FILTER = DeserializerMultiMap(FieldFilterConfiguration::class.java, {it.fieldName}, "fieldName")


open class MqRouterFilterConfigurationDeserializer :
    JsonDeserializer<MqRouterFilterConfiguration>() {

    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): MqRouterFilterConfiguration {
        val location = p.currentLocation
        val node = p.readValueAsTree<JsonNode>()
        val metadata = node["metadata"]?.let { DESERIALIZER_MULTI_MAP_FIELD_FILTER.deserializerMultiMap(p, it, "metadata", location) }
            ?: MultiMapUtils.emptyMultiValuedMap()
        val message = node["message"]?.let { DESERIALIZER_MULTI_MAP_FIELD_FILTER.deserializerMultiMap(p, it, "message", location) }
            ?: MultiMapUtils.emptyMultiValuedMap()

        return MqRouterFilterConfiguration(metadata, message)
    }
}

class MessageRouterConfigurationModule : SimpleModule() {
    init {
        addDeserializer(MqRouterFilterConfiguration::class.java, MqRouterFilterConfigurationDeserializer())
    }
}