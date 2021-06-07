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

package com.exactpro.th2.common.schema.grpc.configuration

import com.exactpro.th2.common.schema.configuration.Configuration
import com.exactpro.th2.common.schema.message.configuration.DESERIALIZER_MULTI_MAP_FIELD_FILTER
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.util.emptyMultiMap
import com.fasterxml.jackson.annotation.JsonGetter
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.commons.collections4.MultiMapUtils
import org.apache.commons.collections4.MultiValuedMap

data class GrpcRawFilterStrategy(var filters: List<GrpcRouterFilterConfiguration?> = emptyList())

data class GrpcRouterFilterConfiguration(
    @JsonProperty(required = true) var endpoint: String,
    override var metadata: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap(),
    override var message: MultiValuedMap<String, FieldFilterConfiguration> = emptyMultiMap()
) : RouterFilter, Configuration() {

    @JsonGetter("metadata")
    fun getJsonMetadata(): Collection<FieldFilterConfiguration> = metadata.values()

    @JsonGetter("message")
    fun getJsonMessage(): Collection<FieldFilterConfiguration> = message.values()

}

class GrpcRouterFilterConfigurationDeserializer : JsonDeserializer<GrpcRouterFilterConfiguration>() {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): GrpcRouterFilterConfiguration {
        val location = p.currentLocation
        val node = p.readValueAsTree<JsonNode>()
        val endpoint: String = node["endpoint"]?.asText() ?: throw JsonParseException(p, "Can not find field with name 'endpoint'", location)
        val metadata = node["metadata"]?.let { DESERIALIZER_MULTI_MAP_FIELD_FILTER.deserializerMultiMap(p, it, "metadata", location) }
            ?: MultiMapUtils.emptyMultiValuedMap()
        val message = node["message"]?.let { DESERIALIZER_MULTI_MAP_FIELD_FILTER.deserializerMultiMap(p, it, "message", location) }
            ?: MultiMapUtils.emptyMultiValuedMap()

        return GrpcRouterFilterConfiguration(endpoint, metadata, message)
    }
}

class GrpcRawFilterStrategyModule : SimpleModule() {
    init {
        addDeserializer(GrpcRouterFilterConfiguration::class.java, GrpcRouterFilterConfigurationDeserializer())
    }
}