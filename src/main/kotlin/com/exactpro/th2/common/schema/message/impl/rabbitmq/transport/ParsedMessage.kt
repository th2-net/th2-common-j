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
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.Collections

class ParsedMessage private constructor(
    override val id: MessageId = MessageId.DEFAULT,
    override val eventId: EventId? = null,
    val type: String,
    override val metadata: Map<String, String> = emptyMap(),
    override val protocol: String = "",
    val rawBody: ByteBuf = Unpooled.buffer(),
    private val bodySupplier: (ByteBuf) -> Map<String, Any?> = DEFAULT_BODY_SUPPLIER,
    body: Map<String, Any?> = DEFAULT_BODY,
) : Message<Map<String, Any?>> {
    constructor(
        id: MessageId = MessageId.DEFAULT,
        eventId: EventId? = null,
        type: String,
        metadata: Map<String, String> = emptyMap(),
        protocol: String = "",
        rawBody: ByteBuf = Unpooled.buffer(),
        bodySupplier: (ByteBuf) -> Map<String, Any?> = DEFAULT_BODY_SUPPLIER,
    ) : this(
        id = id,
        eventId = eventId,
        type = type,
        metadata = metadata,
        protocol = protocol,
        rawBody = rawBody,
        bodySupplier = bodySupplier,
        body = DEFAULT_BODY,
    )

    constructor(
        id: MessageId = MessageId.DEFAULT,
        eventId: EventId? = null,
        type: String,
        metadata: Map<String, String> = emptyMap(),
        protocol: String = "",
        body: Map<String, Any?>,
    ) : this(
        id = id,
        eventId = eventId,
        type = type,
        metadata = metadata,
        protocol = protocol,
        rawBody = Unpooled.buffer(),
        bodySupplier = DEFAULT_BODY_SUPPLIER,
        body = body,
    )

    /**
     * Is set to `true` if the [body] is deserialized from the [rawBody].
     * If the [body] is set directly returns `false`
     */
    val isBodyInRaw: Boolean = body === DEFAULT_BODY

    override val body: Map<String, Any?> by lazy {
        if (body === DEFAULT_BODY) {
            bodySupplier.invoke(rawBody).apply {
                rawBody.resetReaderIndex()
            }
        } else {
            body
        }
    }


    interface Builder<out T : Builder<T>> : Message.Builder<T> {
        val type: String
        fun isTypeSet(): Boolean

        fun setType(type: String): T
        override fun build(): ParsedMessage
    }

    //TODO: add override annotation
    fun toBuilder(): FromMapBuilder {
        return FromMapBuilderImpl()
            .setBody(body)
            .setId(id)
            .setMetadata(metadata)
            .setProtocol(protocol)
            .setType(type)
            .also { eventId?.let(it::setEventId) }
    }

    interface FromRawBuilder : Builder<FromRawBuilder> {
        fun setRawBody(rawBody: ByteBuf): FromRawBuilder
    }

    interface FromMapBuilder : Builder<FromMapBuilder> {
        fun setBody(body: Map<String, Any?>): FromMapBuilder
        fun bodyBuilder(): MapBuilder<String, Any?>
        fun addField(name: String, value: Any?): FromMapBuilder
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ParsedMessage

        if (id != other.id) return false
        if (eventId != other.eventId) return false
        if (type != other.type) return false
        if (metadata != other.metadata) return false
        if (protocol != other.protocol) return false
        return rawBody == other.rawBody
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + (eventId?.hashCode() ?: 0)
        result = 31 * result + type.hashCode()
        result = 31 * result + metadata.hashCode()
        result = 31 * result + protocol.hashCode()
        result = 31 * result + rawBody.hashCode()
        return result
    }

    override fun toString(): String {
        return "ParsedMessage(id=$id, " +
                "eventId=$eventId, " +
                "type='$type', " +
                "metadata=$metadata, " +
                "protocol='$protocol', " +
                "rawBody=${rawBody.toString(Charsets.UTF_8)}, " +
                "body=${
                    if (isBodyInRaw) {
                        "!checkRawBody"
                    } else {
                        body.toString()
                    }
                })"
    }


    companion object {
        @JvmField
        val EMPTY: ParsedMessage = ParsedMessage(type = "", body = emptyMap())

        @Deprecated(
            "Please use EMPTY instead. Added for binary compatibility",
            level = DeprecationLevel.HIDDEN,
            replaceWith = ReplaceWith(expression = "EMPTY")
        )
        @JvmStatic
        fun getEMPTY(): ParsedMessage = EMPTY

        /**
         * We want to be able to identify the default body by reference.
         * So, that is why we use unmodifiableMap with emptyMap
         * Otherwise, we won't be able to identify it
         */
        private val DEFAULT_BODY: Map<String, Any?> = Collections.unmodifiableMap(emptyMap())

        @JvmField
        val DEFAULT_BODY_SUPPLIER: (ByteBuf) -> Map<String, Any?> = { emptyMap() }

        @JvmStatic
        fun builder(bodySupplier: (ByteBuf) -> Map<String, Any?>): FromRawBuilder = FromRawBuilderImpl(bodySupplier)

        @JvmStatic
        fun builder(): FromMapBuilder = FromMapBuilderImpl()
    }
}

@Suppress("PropertyName")
private sealed class BaseParsedBuilder<out T : ParsedMessage.Builder<T>> : ParsedMessage.Builder<T> {
    protected var idBuilder: MessageId.Builder? = null
    protected var id: MessageId? = MessageId.DEFAULT
    final override var eventId: EventId? = null
        private set
    protected var _protocol: String? = null
    protected var _type: String? = null
    protected var metadataBuilder: MapBuilder<String, String>? = null
    protected var metadata: Map<String, String>? = emptyMap()

    override val protocol: String
        get() = this._protocol ?: ""

    override fun isProtocolSet(): Boolean = _protocol != null
    override val type: String
        get() = requireNotNull(this._type) {
            "Property \"type\" has not been set"
        }

    override fun isTypeSet(): Boolean = _type != null

    override fun setId(id: MessageId): T = self {
        require(idBuilder == null) {
            "cannot set id after calling idBuilder()"
        }
        this.id = id
    }

    override fun idBuilder(): MessageId.Builder {
        if (idBuilder == null) {
            idBuilder = id?.toBuilder()?.also {
                id = null
            } ?: MessageId.builder()
        }
        return requireNotNull(idBuilder) { "idBuilder is null" }
    }

    override fun setEventId(eventId: EventId): T = self {
        this.eventId = eventId
    }

    override fun setProtocol(protocol: String): T = self {
        this._protocol = protocol
    }

    override fun setMetadata(metadata: Map<String, String>): T = self {
        require(metadataBuilder == null) {
            "cannot set metadata after calling metadataBuilder()"
        }
        this.metadata = metadata
    }

    override fun setType(type: String): T = self {
        this._type = type
    }

    override fun metadataBuilder(): MapBuilder<String, String> {
        if (metadataBuilder == null) {
            metadataBuilder = metadata?.let {
                metadata = null
                MapBuilder<String, String>().putAll(it)
            } ?: MapBuilder()
        }
        return requireNotNull(metadataBuilder) { "metadataBuilder is null" }
    }

    override fun addMetadataProperty(key: String, value: String): T = self {
        metadataBuilder().put(key, value)
    }

    @Suppress("UNCHECKED_CAST")
    private inline fun self(block: BaseParsedBuilder<T>.() -> Unit): T {
        block()
        return this as T
    }
}

private class FromRawBuilderImpl(
    private val bodySupplier: (ByteBuf) -> Map<String, Any?>,
) : BaseParsedBuilder<ParsedMessage.FromRawBuilder>(), ParsedMessage.FromRawBuilder {
    private var rawBody: ByteBuf? = null
    override fun setRawBody(rawBody: ByteBuf): ParsedMessage.FromRawBuilder = apply {
        this.rawBody = rawBody
    }

    override fun build(): ParsedMessage = ParsedMessage(
        id = id ?: idBuilder?.build() ?: error("missing id"),
        eventId = eventId,
        type = _type ?: error("missing type"),
        metadata = metadata ?: metadataBuilder?.build() ?: emptyMap(),
        protocol = _protocol ?: "",
        rawBody = rawBody ?: error("missing raw body"),
        bodySupplier = bodySupplier,
    )
}

private class FromMapBuilderImpl : BaseParsedBuilder<ParsedMessage.FromMapBuilder>(), ParsedMessage.FromMapBuilder {
    private var body: Map<String, Any?>? = null
    private var bodyBuilder: MapBuilder<String, Any?>? = null
    override fun setBody(body: Map<String, Any?>): ParsedMessage.FromMapBuilder = apply {
        require(bodyBuilder == null) {
            "cannot set body after calling bodyBuilder()"
        }
        this.body = body
    }

    override fun bodyBuilder(): MapBuilder<String, Any?> {
        if (bodyBuilder == null) {
            bodyBuilder = body?.let {
                body = null
                MapBuilder<String, Any?>().putAll(it)
            } ?: MapBuilder()
        }
        return requireNotNull(bodyBuilder) { "bodyBuilder is null" }
    }

    override fun addField(name: String, value: Any?): ParsedMessage.FromMapBuilder = apply {
        bodyBuilder().put(name, value)
    }

    override fun build(): ParsedMessage = ParsedMessage(
        id = id ?: idBuilder?.build() ?: error("missing id"),
        eventId = eventId,
        type = _type ?: error("missing type"),
        metadata = metadata ?: metadataBuilder?.build() ?: emptyMap(),
        protocol = _protocol ?: "",
        body = body ?: bodyBuilder?.build() ?: error("missing body"),
    )
}