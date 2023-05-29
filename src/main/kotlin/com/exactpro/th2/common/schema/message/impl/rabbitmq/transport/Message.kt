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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.transport

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder

interface Message<T> {
    /** The id is not mutable by default */
    val id: MessageId
    val eventId: EventId?

    /** The metadata is not mutable by default */
    val metadata: Map<String, String>
    val protocol: String
    val body: T

    interface Builder<out T : Builder<T>> {
        val protocol: String

        fun setId(id: MessageId): T
        fun idBuilder(): MessageId.Builder
        fun setEventId(eventId: EventId): T
        fun setProtocol(protocol: String): T
        fun setMetadata(metadata: Map<String, String>): T
        fun metadataBuilder(): MapBuilder<String, String>

        fun addMetadataProperty(key: String, value: String): T
        fun build(): Message<*>
    }
}