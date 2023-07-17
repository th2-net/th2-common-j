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

import com.google.auto.value.AutoBuilder
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.buffer.Unpooled

data class RawMessage(
    override val id: MessageId = MessageId.DEFAULT,
    override val eventId: EventId? = null,
    override val metadata: Map<String, String> = emptyMap(),
    override val protocol: String = "",
    /** The body is not mutable by default */
    override val body: ByteBuf = Unpooled.EMPTY_BUFFER,
) : Message<ByteBuf> {


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RawMessage

        if (id != other.id) return false
        if (eventId != other.eventId) return false
        if (metadata != other.metadata) return false
        if (protocol != other.protocol) return false
        return body == other.body
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + (eventId?.hashCode() ?: 0)
        result = 31 * result + metadata.hashCode()
        result = 31 * result + protocol.hashCode()
        result = 31 * result + body.hashCode()
        return result
    }

    override fun toString(): String {
        return "RawMessage(id=$id, eventId=$eventId, metadata=$metadata, protocol='$protocol', body=${hexDump(body)})"
    }

    @AutoBuilder
    interface Builder : Message.Builder<Builder> {

        val body: ByteBuf
        fun setBody(body: ByteBuf): Builder

        fun setBody(data: ByteArray): Builder = setBody(Unpooled.wrappedBuffer(data))

        override fun addMetadataProperty(key: String, value: String): Builder = this.apply {
            metadataBuilder().put(key, value)
        }

        override fun build(): RawMessage
    }

    fun toBuilder(): Builder = AutoBuilder_RawMessage_Builder(this)

    companion object {

        val EMPTY = RawMessage()

        @JvmStatic
        fun builder(): Builder = AutoBuilder_RawMessage_Builder()
    }
}