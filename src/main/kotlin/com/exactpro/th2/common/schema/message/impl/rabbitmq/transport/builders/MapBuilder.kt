/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders

class MapBuilder<K, V>(
    private val innerMap: MutableMap<K, V> = hashMapOf(),
) {
    val size: Int
        get() = innerMap.size

    fun contains(key: K): Boolean = innerMap.contains(key)
    operator fun get(key: K): V? = innerMap[key]
    fun put(key: K, value: V): MapBuilder<K, V> = apply {
        innerMap[key] = value
    }

    fun putAll(from: Map<K, V>): MapBuilder<K, V> = apply {
        innerMap.putAll(from)
    }

    fun build(): Map<K, V> {
        return innerMap
    }
}