/*
 * Copyright 2023-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.GenericCollectionBuilder
import com.google.auto.value.AutoBuilder


data class MessageGroup(
    val messages: List<Message<*>> = emptyList(), // FIXME: message can have incompatible book and group
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MessageGroup

        return messages == other.messages
    }

    override fun hashCode(): Int {
        return messages.hashCode()
    }

    override fun toString(): String {
        return "MessageGroup(messages=$messages)"
    }

    // We cannot use AutoBuilder here because of the different method signatures in builder when a generic type is used
    //
    @AutoBuilder
    interface Builder {
        fun messagesBuilder(): GenericCollectionBuilder<Message<*>>
        fun addMessage(message: Message<*>): Builder = apply {
            messagesBuilder().add(message)
        }

        fun setMessages(message: List<Message<*>>): Builder
        fun build(): MessageGroup
    }

    //TODO: add override annotation
    fun toBuilder(): Builder = builder().setMessages(messages)

    class CollectionBuilder {
        private val elements: MutableList<Message<*>> = mutableListOf()

        fun add(el: Message<*>): CollectionBuilder = apply {
            elements += el
        }

        fun addAll(vararg els: Message<*>): CollectionBuilder = apply {
            for (el in els) {
                add(el)
            }
        }

        fun addAll(elements: Collection<Message<*>>): CollectionBuilder = apply {
            this.elements.addAll(elements)
        }

        fun build(): List<Message<*>> = elements
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = AutoBuilder_MessageGroup_Builder()
    }
}