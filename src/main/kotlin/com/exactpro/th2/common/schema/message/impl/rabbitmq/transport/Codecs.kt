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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.ByteBufOutputStream
import io.netty.buffer.ByteBufUtil
import java.io.OutputStream
import java.nio.charset.Charset
import java.time.Instant

// TODO: maybe make length field a variable length int

@Suppress("unused")
enum class ValueType(val codec: ValueCodec<*>) {
    UNKNOWN(UnknownValueCodec),
    LONG_TYPE(LongTypeCodec),
    STRING_TYPE(StringTypeCodec),
    MESSAGE_ID(MessageIdCodec),
    BOOK(BookCodec),
    SESSION_GROUP(SessionGroupCodec),
    SESSION_ALIAS(SessionAliasCodec),
    DIRECTION(DirectionCodec),
    SEQUENCE(SequenceCodec),
    SUBSEQUENCE(SubsequenceCodec),
    TIMESTAMP(TimestampCodec),
    METADATA(MetadataCodec),
    PROTOCOL(ProtocolCodec),
    MESSAGE_TYPE(MessageTypeCodec),
    ID_CODEC(IdCodec),
    SCOPE_CODEC(ScopeCodec),
    EVENT_ID_CODEC(EventIdCodec),
    RAW_MESSAGE(RawMessageCodec),
    RAW_MESSAGE_BODY(RawMessageBodyCodec),
    PARSED_MESSAGE(ParsedMessageCodec),
    PARSED_MESSAGE_BODY(ParsedMessageRawBodyCodec),
    MESSAGE_GROUP(MessageGroupCodec),
    MESSAGE_LIST(MessageListCodec),
    GROUP_BATCH(GroupBatchCodec),
    GROUP_LIST(GroupListCodec);

    companion object {
        fun forId(id: UByte): ValueType = MAPPING[id.toInt()] ?: UNKNOWN

        private val MAPPING: Array<ValueType?> = arrayOfNulls<ValueType>(UByte.MAX_VALUE.toInt()).apply {
            ValueType.values().forEach {
                this[it.codec.type.toInt()]?.let { previous ->
                    error("$previous and $it elements of ValueType enum have the same type byte - ${it.codec.type}")
                }
                this[it.codec.type.toInt()] = it
            }
        }
    }
}

sealed interface ValueCodec<T> {
    val type: UByte
    fun encode(source: T, target: ByteBuf)
    fun decode(source: ByteBuf): T
}

object UnknownValueCodec : ValueCodec<ByteBuf> {
    override val type: UByte = 0u
    override fun decode(source: ByteBuf): ByteBuf = source.readSlice(source.skipBytes(Byte.SIZE_BYTES).readIntLE())
    override fun encode(source: ByteBuf, target: ByteBuf): Nothing = throw UnsupportedOperationException()
}

abstract class AbstractCodec<T>(final override val type: UByte) : ValueCodec<T> {
    override fun encode(source: T, target: ByteBuf) {
        val lengthIndex = target.writeByte(type.toInt()).writerIndex()
        target.writeIntLE(0)
        val valueIndex = target.writerIndex()
        write(target, source)
        target.setIntLE(lengthIndex, target.writerIndex() - valueIndex)
    }

    protected abstract fun write(buffer: ByteBuf, value: T)

    override fun decode(source: ByteBuf): T {
        val tag = source.readByte().toUByte()
        check(tag == this.type) { "Unexpected type tag: $tag (expected: ${this.type})" }
        val length = source.readIntLE()
        return read(source.readSlice(length)) // FIXME: avoid slicing to avoid buffer allocation
    }

    protected abstract fun read(buffer: ByteBuf): T
}

abstract class StringCodec(
    type: UByte,
    private val charset: Charset = Charsets.UTF_8,
) : AbstractCodec<String>(type) {
    override fun read(buffer: ByteBuf): String = buffer.readCharSequence(buffer.readableBytes(), charset).toString()

    override fun write(buffer: ByteBuf, value: String) {
        buffer.writeCharSequence(value, charset)
    }
}

abstract class LongCodec(type: UByte) : AbstractCodec<Long>(type) {
    override fun read(buffer: ByteBuf): Long = buffer.readLongLE()

    override fun write(buffer: ByteBuf, value: Long) {
        buffer.writeLongLE(value)
    }
}

abstract class IntCodec(type: UByte) : AbstractCodec<Int>(type) {
    override fun read(buffer: ByteBuf): Int = buffer.readIntLE()

    override fun write(buffer: ByteBuf, value: Int) {
        buffer.writeIntLE(value)
    }
}

abstract class ListCodec<T>(type: UByte, private val elementCodec: ValueCodec<T>) : AbstractCodec<List<T>>(type) {
    override fun read(buffer: ByteBuf): MutableList<T> = mutableListOf<T>().also { list ->
        while (buffer.isReadable) {
            list += elementCodec.decode(buffer)
        }
    }

    override fun write(buffer: ByteBuf, value: List<T>) {
        value.forEach { elementCodec.encode(it, buffer) }
    }
}

abstract class MapCodec<K, V>(
    type: UByte,
    private val keyCodec: ValueCodec<K>,
    private val valueCodec: ValueCodec<V>,
) : AbstractCodec<Map<K, V>>(type) {
    override fun read(buffer: ByteBuf): MutableMap<K, V> = hashMapOf<K, V>().apply {
        while (buffer.isReadable) {
            this[keyCodec.decode(buffer)] = valueCodec.decode(buffer)
        }
    }

    override fun write(buffer: ByteBuf, value: Map<K, V>): Unit = value.forEach { (key, value) ->
        keyCodec.encode(key, buffer)
        valueCodec.encode(value, buffer)
    }
}

abstract class ByteBufCodec(type: UByte) : AbstractCodec<ByteBuf>(type) {
    override fun read(buffer: ByteBuf): ByteBuf = buffer.copy()

    override fun write(buffer: ByteBuf, value: ByteBuf) {
        value.markReaderIndex().apply(buffer::writeBytes).resetReaderIndex()
    }
}

abstract class InstantCodec(type: UByte) : AbstractCodec<Instant>(type) {
    override fun read(buffer: ByteBuf): Instant = Instant.ofEpochSecond(buffer.readLongLE(), buffer.readIntLE().toLong())

    override fun write(buffer: ByteBuf, value: Instant) {
        buffer.writeLongLE(value.epochSecond).writeIntLE(value.nano)
    }
}

// FIXME: think about checking that type is unique
object LongTypeCodec : LongCodec(1u)

object StringTypeCodec : StringCodec(2u)

object IntTypeCodec : IntCodec(3u)

object MessageIdCodec : AbstractCodec<MessageId>(10u) {
    override fun read(buffer: ByteBuf): MessageId = MessageId.builder().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is SessionAliasCodec -> setSessionAlias(codec.decode(buffer))
                is DirectionCodec -> setDirection(codec.decode(buffer))
                is SequenceCodec -> setSequence(codec.decode(buffer))
                is SubsequenceCodec -> setSubsequence(codec.decode(buffer))
                is TimestampCodec -> setTimestamp(codec.decode(buffer))
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }.build()

    override fun write(buffer: ByteBuf, value: MessageId) {
        SessionAliasCodec.encode(value.sessionAlias, buffer)
        DirectionCodec.encode(value.direction, buffer)
        SequenceCodec.encode(value.sequence, buffer)
        SubsequenceCodec.encode(value.subsequence, buffer)
        TimestampCodec.encode(value.timestamp, buffer)
    }
}

object BookCodec : StringCodec(101u)

object SessionGroupCodec : StringCodec(102u)

object SessionAliasCodec : StringCodec(103u)

object DirectionCodec : AbstractCodec<Direction>(104u) {
    override fun read(buffer: ByteBuf): Direction = Direction.forId(buffer.readByte().toInt())

    override fun write(buffer: ByteBuf, value: Direction) {
        buffer.writeByte(value.id)
    }
}

object SequenceCodec : LongCodec(105u)

object SubsequenceCodec : ListCodec<Int>(106u, IntTypeCodec)

object TimestampCodec : InstantCodec(107u)

object MetadataCodec : MapCodec<String, String>(11u, StringTypeCodec, StringTypeCodec)

object ProtocolCodec : StringCodec(12u)

object MessageTypeCodec : StringCodec(13u)

object IdCodec : StringCodec(14u)

object ScopeCodec : StringCodec(15u)

object EventIdCodec : AbstractCodec<EventId>(16u) {
    override fun read(buffer: ByteBuf): EventId {
        return EventId.builder().apply {
            buffer.forEachValue { codec ->
                when (codec) {
                    is IdCodec -> setId(codec.decode(buffer))
                    is BookCodec -> setBook(codec.decode(buffer))
                    is ScopeCodec -> setScope(codec.decode(buffer))
                    is TimestampCodec -> setTimestamp(codec.decode(buffer))
                    else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
                }
            }
        }.build()
    }

    override fun write(buffer: ByteBuf, value: EventId) {
        IdCodec.encode(value.id, buffer)
        BookCodec.encode(value.book, buffer)
        ScopeCodec.encode(value.scope, buffer)
        TimestampCodec.encode(value.timestamp, buffer)
    }
}

object RawMessageCodec : AbstractCodec<RawMessage>(20u) {
    override fun read(buffer: ByteBuf): RawMessage = RawMessage.builder().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is MessageIdCodec -> setId(codec.decode(buffer))
                is EventIdCodec -> setEventId(codec.decode(buffer))
                is MetadataCodec -> setMetadata(codec.decode(buffer))
                is ProtocolCodec -> setProtocol(codec.decode(buffer))
                is RawMessageBodyCodec -> setBody(codec.decode(buffer))
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }.build()

    override fun write(buffer: ByteBuf, value: RawMessage) {
        MessageIdCodec.encode(value.id, buffer)
        value.eventId?.run { EventIdCodec.encode(this, buffer) }
        MetadataCodec.encode(value.metadata, buffer)
        ProtocolCodec.encode(value.protocol, buffer)
        RawMessageBodyCodec.encode(value.body, buffer)
    }
}

object RawMessageBodyCodec : ByteBufCodec(21u)

object ParsedMessageCodec : AbstractCodec<ParsedMessage>(30u) {
    override fun read(buffer: ByteBuf): ParsedMessage = ParsedMessage.builder { buf ->
        ByteBufInputStream(buf).use { MAPPER.readValue(it) }
    }.apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is MessageIdCodec -> setId(codec.decode(buffer))
                is EventIdCodec -> setEventId(codec.decode(buffer))
                is MetadataCodec -> setMetadata(codec.decode(buffer))
                is ProtocolCodec -> setProtocol(codec.decode(buffer))
                is MessageTypeCodec -> setType(codec.decode(buffer))
                is ParsedMessageRawBodyCodec -> setRawBody(codec.decode(buffer))
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }.build()

    override fun write(buffer: ByteBuf, value: ParsedMessage) {
        MessageIdCodec.encode(value.id, buffer)
        value.eventId?.run { EventIdCodec.encode(this, buffer) }
        MetadataCodec.encode(value.metadata, buffer)
        ProtocolCodec.encode(value.protocol, buffer)
        MessageTypeCodec.encode(value.type, buffer)
        if (!value.isBodyInRaw) {
            // Update raw body because the body was changed
            ByteBufOutputStream(value.rawBody.clear()).use {
                MAPPER.writeValue(it as OutputStream, value.body)
            }
            value.rawBody.resetReaderIndex()
        }
        ParsedMessageRawBodyCodec.encode(value.rawBody, buffer)
    }

    @JvmField
    val MAPPER: ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
        // otherwise, type supported by JavaTimeModule will be serialized as array of date component
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        // this is required to serialize nulls and empty collections
        .setSerializationInclusion(JsonInclude.Include.ALWAYS)
}

object ParsedMessageRawBodyCodec : ByteBufCodec(31u)

object MessageGroupCodec : AbstractCodec<MessageGroup>(40u) {
    override fun read(buffer: ByteBuf): MessageGroup = MessageGroup.builder().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is MessageListCodec -> setMessages(codec.decode(buffer))
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }.build()

    override fun write(buffer: ByteBuf, value: MessageGroup) {
        MessageListCodec.encode(value.messages, buffer)
    }
}

object MessageListCodec : AbstractCodec<List<Message<*>>>(41u) {
    override fun read(buffer: ByteBuf): MutableList<Message<*>> = mutableListOf<Message<*>>().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is RawMessageCodec -> this += codec.decode(buffer)
                is ParsedMessageCodec -> this += codec.decode(buffer)
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }

    override fun write(buffer: ByteBuf, value: List<Message<*>>): Unit = value.forEach { message ->
        when (message) {
            is RawMessage -> RawMessageCodec.encode(message, buffer)
            is ParsedMessage -> ParsedMessageCodec.encode(message, buffer)
            else -> println("Skipping unsupported message type: $message")
        }
    }
}

object GroupBatchCodec : AbstractCodec<GroupBatch>(50u) {
    override fun read(buffer: ByteBuf): GroupBatch = GroupBatch.builder().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is BookCodec -> setBook(codec.decode(buffer))
                is SessionGroupCodec -> setSessionGroup(codec.decode(buffer))
                is GroupListCodec -> setGroups(codec.decode(buffer))
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }.build()

    override fun write(buffer: ByteBuf, value: GroupBatch) {
        BookCodec.encode(value.book, buffer)
        SessionGroupCodec.encode(value.sessionGroup, buffer)
        GroupListCodec.encode(value.groups, buffer)
    }
}

object GroupListCodec : ListCodec<MessageGroup>(51u, MessageGroupCodec)

inline fun ByteBuf.forEachValue(action: (codec: ValueCodec<*>) -> Unit) {
    while (isReadable) {
        val type = getByte(readerIndex()).toUByte()

        when (val codec = ValueType.forId(type).codec) {
            is UnknownValueCodec -> println("Skipping unknown type $type value: ${ByteBufUtil.hexDump(codec.decode(this))}")
            else -> action(codec)
        }
    }
}