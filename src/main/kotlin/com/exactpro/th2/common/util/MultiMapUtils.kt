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

package com.exactpro.th2.common.util

import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfigurationOld
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import org.apache.commons.collections4.MultiMapUtils
import org.apache.commons.collections4.MultiValuedMap

fun <K, V> multiMapOf(vararg pairs: Pair<K, V>): MultiValuedMap<K, V> = multiMapOf(pairs.toList())

fun <K, V> multiMapOf(list: List<Pair<K, V>>): MultiValuedMap<K, V> =
    MultiMapUtils.newListValuedHashMap<K, V>().also { multiMap ->
        list.forEach { (key, value) ->
            multiMap.put(key, value)
        }
    }

fun <K, V> emptyMultiMap(): MultiValuedMap<K, V> = MultiMapUtils.newListValuedHashMap()

class MultiMapFiltersDeserializer : JsonDeserializer<MultiValuedMap<String, FieldFilterConfiguration>>() {
    override fun deserialize(parser: JsonParser, ctxt: DeserializationContext): MultiValuedMap<String, FieldFilterConfiguration> {
        val node = parser.readValueAsTree<JsonNode>()

        val result: MultiValuedMap<String, FieldFilterConfiguration> = MultiMapUtils.newListValuedHashMap()
        val codec = parser.codec

        when {
            node.isArray -> node.forEach { element ->
                codec.treeToValue(element, FieldFilterConfiguration::class.java)?.also { filter ->
                    result.put(filter.fieldName, filter)
                }
            }
            node.isObject -> {
                val mapDeserializer = ctxt.findRootValueDeserializer(ctxt.typeFactory.constructType(object : TypeReference<Map<String, FieldFilterConfigurationOld>>(){}))
                val nodeParser = parser.codec.treeAsTokens(node)
                nodeParser.nextToken()
                val map = mapDeserializer.deserialize(nodeParser, ctxt) as Map<String, FieldFilterConfigurationOld>?

                map?.entries?.forEach { (fieldName, filter) ->
                    result.put(fieldName, FieldFilterConfiguration(fieldName, filter.value, filter.operation))
                }
            }
            else -> ctxt.reportWrongTokenException(MultiValuedMap::class.java, JsonToken.START_ARRAY, "Can not deserialize MultiValuedMap. Field is not array or object.")
        }
        return result;
    }

}

