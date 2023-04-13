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

data class RawMessage(
    override var id: MessageId = MessageId.DEFAULT_INSTANCE,
    override var eventId: EventId? = null,
    override var metadata: MutableMap<String, String> = Message.DEFAULT_METADATA,
    override var protocol: String = "",
    /** The body is not mutable by default */
    override var body: ByteBuf = Unpooled.EMPTY_BUFFER,
) : Message<ByteBuf> {
    override fun clean() {
        check(id !== MessageId.DEFAULT_INSTANCE) {
            "Object can be cleaned because 'id' is default instance"
        }
        check(metadata !== Message.DEFAULT_METADATA) {
            "Object can be cleaned because 'metadata' is immutable"
        }
        check(body !== Unpooled.EMPTY_BUFFER) {
            "Object can be cleaned because 'body' is immutable"
        }

        id.clean()
        eventId = null
        metadata.clear()
        protocol = ""
        body.clear()
    }

    override fun softClean() {
        check(metadata !== Message.DEFAULT_METADATA) {
            "Object can be cleaned because 'metadata' is immutable"
        }

        id = MessageId.DEFAULT_INSTANCE
        eventId = null
        metadata.clear()
        protocol = ""
        body = Unpooled.EMPTY_BUFFER
    }

    companion object {
        fun newMutable() = RawMessage(
            id = MessageId.newMutable(),
            metadata = mutableMapOf(),
            body = Unpooled.buffer()
        )
        fun newSoftMutable() = RawMessage(
            metadata = mutableMapOf()
        )
    }
}