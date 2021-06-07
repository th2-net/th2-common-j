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

import com.fasterxml.jackson.core.JsonLocation
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import org.apache.commons.collections4.MultiMapUtils
import org.apache.commons.collections4.MultiValuedMap

fun <K, V> multiMapOf(vararg pairs: Pair<K, V>): MultiValuedMap<K, V> =
    MultiMapUtils.newListValuedHashMap<K, V>().also { multiMap ->
        pairs.forEach { (key, value) ->
            multiMap.put(key, value)
        }
    }

fun <K, V> multiMapOf(list: List<Pair<K, V>>): MultiValuedMap<K, V> =
    MultiMapUtils.newListValuedHashMap<K, V>().also { multiMap ->
        list.forEach { (key, value) ->
            multiMap.put(key, value)
        }
    }

fun <K, V> emptyMultiMap(): MultiValuedMap<K, V> = MultiMapUtils.newListValuedHashMap()

class DeserializerMultiMap<T>(
    private val valueClass: Class<T>,
    private val keySupplier: (T) -> String,
    private val fieldKeyName: String? = null
) {
    fun deserializerMultiMap(
        parser: JsonParser,
        node: JsonNode,
        name: String,
        location: JsonLocation
    ): MultiValuedMap<String, T> {
        val result: MultiValuedMap<String, T> = MultiMapUtils.newListValuedHashMap()
        val codec = parser.codec
        when {
            node.isArray -> node.forEach { element ->
                codec.treeToValue(element, valueClass)?.also { obj ->
                    result.put(keySupplier(obj), obj)
                }
            }
            node.isObject -> node.fields().forEach { field ->
                val fieldName = field.key

                val fullNode = if (fieldKeyName != null && field.value is ObjectNode) (field.value as ObjectNode).set(
                    fieldKeyName,
                    TextNode(fieldName)
                ) else node

                codec.treeToValue(fullNode, valueClass)?.also { obj ->
                    result.put(keySupplier(obj), obj)
                }
            }
            else -> throw JsonParseException(parser, "Can not deserialize field with name '$name'", location)
        }
        return result;
    }
}

