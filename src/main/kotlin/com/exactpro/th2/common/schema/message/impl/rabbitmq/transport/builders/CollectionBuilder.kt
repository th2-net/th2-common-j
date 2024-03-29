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

class CollectionBuilder<T> {
    private val elements: MutableList<T> = mutableListOf()

    val size: Int
        get() = elements.size

    fun isEmpty(): Boolean = elements.isEmpty()

    operator fun get(index: Int): T = elements[index]

    fun add(el: T): CollectionBuilder<T> = apply {
        elements += el
    }

    fun addAll(vararg els: T): CollectionBuilder<T> = apply {
        for (el in els) {
            add(el)
        }
    }

    fun addAll(elements: Collection<T>): CollectionBuilder<T> = apply {
        this.elements.addAll(elements)
    }

    fun build(): List<T> = elements
}