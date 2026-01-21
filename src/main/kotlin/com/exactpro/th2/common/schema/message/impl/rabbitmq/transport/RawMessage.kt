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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId.Companion.builder
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
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

    interface Builder : Message.Builder<Builder> {

        val body: ByteBuf
        fun isBodySet(): Boolean
        fun setBody(body: ByteBuf): Builder

        fun setBody(data: ByteArray): Builder = setBody(Unpooled.wrappedBuffer(data))

        override fun addMetadataProperty(key: String, value: String): Builder = this.apply {
            metadataBuilder().put(key, value)
        }

        override fun build(): RawMessage
    }

    fun toBuilder(): Builder = RawMessageBuilderImpl(this)

    companion object {

        @JvmField
        val EMPTY: RawMessage = RawMessage()

        @Deprecated(
            "Please use EMPTY instead. Added for binary compatibility",
            level = DeprecationLevel.HIDDEN,
            replaceWith = ReplaceWith(expression = "EMPTY")
        )
        @JvmStatic
        fun getEMPTY(): RawMessage = EMPTY

        @JvmStatic
        fun builder(): Builder = RawMessageBuilderImpl()
    }
}


internal class RawMessageBuilderImpl : RawMessage.Builder {
    private var _idBuilder: MessageId.Builder? = null
    private var _id: MessageId? = null
    private var _eventId: EventId? = null
    private var _metadataBuilder: MapBuilder<String, String>? = null
    private var metadata: Map<String, String>? = null
    private var _protocol: String? = null
    private var _body: ByteBuf? = null

    constructor()
    constructor(source: RawMessage) {
        _id = source.id
        _eventId = source.eventId
        metadata = source.metadata
        _protocol = source.protocol
        _body = source.body
    }

    override fun setId(id: MessageId): RawMessage.Builder = apply {
        check(_idBuilder == null) { "Cannot set id after calling idBuilder()" }
        this._id = id
    }

    override fun idBuilder(): MessageId.Builder {
        if (_idBuilder == null) {
            _idBuilder = _id?.toBuilder()?.also {
                _id = null
            } ?: builder()
        }
        return _idBuilder!!
    }

    override fun setEventId(eventId: EventId): RawMessage.Builder = apply {
        this._eventId = eventId
        return this
    }

    override val eventId: EventId?
        get() = _eventId

    override fun setMetadata(metadata: Map<String, String>): RawMessage.Builder = apply {
        check(_metadataBuilder == null) { "Cannot set metadata after calling metadataBuilder()" }
        this.metadata = metadata
    }

    override fun metadataBuilder(): MapBuilder<String, String> {
        if (_metadataBuilder == null) {
            if (metadata == null) {
                _metadataBuilder = MapBuilder()
            } else {
                _metadataBuilder = MapBuilder<String, String>().apply {
                    metadata?.let(this::putAll)
                }
                metadata = null
            }
        }
        return _metadataBuilder!!
    }

    override fun setProtocol(protocol: String): RawMessage.Builder = apply {
        this._protocol = protocol
    }

    override val protocol: String
        get() {
            return checkNotNull(_protocol) { "Property \"protocol\" has not been set" }
        }

    override fun isProtocolSet(): Boolean = _protocol != null

    override fun setBody(body: ByteBuf): RawMessage.Builder = apply {
        this._body = body
    }

    override val body: ByteBuf
        get() = checkNotNull(_body) { "Property \"body\" has not been set" }

    override fun isBodySet(): Boolean = _body != null

    override fun build(): RawMessage {
        _id = _idBuilder?.build()
            ?: _id
        metadata = _metadataBuilder?.build()
            ?: metadata
        return RawMessage(
            _id ?: MessageId.DEFAULT,
            _eventId,
            metadata ?: emptyMap(),
            _protocol ?: "",
            _body ?: Unpooled.EMPTY_BUFFER,
        )
    }
}

