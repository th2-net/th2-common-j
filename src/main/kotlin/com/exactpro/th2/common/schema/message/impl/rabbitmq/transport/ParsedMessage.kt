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

import java.util.*

data class ParsedMessage(
    override var id: MessageId = MessageId.DEFAULT_INSTANCE,
    override var eventId: EventId? = null,
    override var metadata: MutableMap<String, String> = Message.DEFAULT_METADATA,
    override var protocol: String = "",
    var type: String = "",
    /** The body is not mutable by default */
    override var body: MutableMap<String, Any> = DEFAULT_BODY, // FIXME: should be lazy deserializing
) : Message<MutableMap<String, Any>> {
    override fun clean() {
        check(id !== MessageId.DEFAULT_INSTANCE) {
            "Object can be cleaned because 'id' is default instance"
        }
        check(metadata !== Message.DEFAULT_METADATA) {
            "Object can be cleaned because 'metadata' is immutable"
        }
        check(body !== DEFAULT_BODY) {
            "Object can be cleaned because 'body' is immutable"
        }

        id.clean()
        eventId = null
        metadata.clear()
        protocol = ""
        type = ""
        body.clear()
    }

    override fun softClean() {
        check(metadata !== Message.DEFAULT_METADATA) {
            "Object can be cleaned because 'metadata' is immutable"
        }
        check(body !== DEFAULT_BODY) {
            "Object can be cleaned because 'body' is immutable"
        }

        id = MessageId.DEFAULT_INSTANCE
        eventId = null
        metadata.clear()
        protocol = ""
        type = ""
        body.clear()
    }

    companion object {
        val DEFAULT_BODY: MutableMap<String, Any> = Collections.emptyMap()
        @JvmStatic
        fun newMutable() = ParsedMessage(
            id = MessageId.newMutable(),
            metadata = mutableMapOf(),
            body = mutableMapOf()
        )
        @JvmStatic
        fun newSoftMutable() = ParsedMessage(
            metadata = mutableMapOf(),
            body = mutableMapOf()
        )
    }
}