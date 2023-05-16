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

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.*

data class ParsedMessage(
    override var id: MessageId = MessageId.DEFAULT_INSTANCE,
    override var eventId: EventId? = null,
    override var metadata: MutableMap<String, String> = Message.DEFAULT_METADATA,
    override var protocol: String = "",
    var type: String = "",
    var rawBody: ByteBuf = Unpooled.EMPTY_BUFFER,
) : Message<MutableMap<String, Any>> {
    internal var bodySupplier: (ByteBuf) -> MutableMap<String, Any> = { DEFAULT_BODY }

    private var _body: MutableMap<String, Any>? = null
    /** The body is not mutable by default */
    override val body: MutableMap<String, Any>
        get() {
            if (_body == null) {
                _body = bodySupplier(rawBody)
            }
            return requireNotNull(_body) { "body is null" }
        }
    override fun clean() {
        check(id !== MessageId.DEFAULT_INSTANCE) {
            "Object can be cleaned because 'id' is default instance"
        }
        check(metadata !== Message.DEFAULT_METADATA) {
            "Object can be cleaned because 'metadata' is immutable"
        }
        check(rawBody !== Unpooled.EMPTY_BUFFER) {
            "Object can be cleaned because 'rawBody' is immutable"
        }

        id.clean()
        eventId = null
        metadata.clear()
        protocol = ""
        type = ""
        rawBody.clear()
        _body = null
    }

    companion object {
        val DEFAULT_BODY: MutableMap<String, Any> = Collections.emptyMap()
        @JvmStatic
        fun newMutable() = ParsedMessage(
            id = MessageId.newMutable(),
            metadata = hashMapOf(),
            rawBody = Unpooled.buffer(),
        )
    }
}