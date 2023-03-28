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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.demo

import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection.OUTGOING
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufInputStream
import io.netty.buffer.ByteBufOutputStream
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.Charset
import java.time.Instant
import java.util.Objects

// TODO: maybe make length field a variable length int

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
    RAW_MESSAGE(RawMessageCodec),
    RAW_MESSAGE_BODY(RawMessageBodyCodec),
    PARSED_MESSAGE(ParsedMessageCodec),
    MESSAGE_GROUP(MessageGroupCodec),
    MESSAGE_LIST(MessageListCodec),
    GROUP_BATCH(GroupBatchCodec),
    GROUP_LIST(GroupListCodec);

    companion object {
        private val MAPPING = arrayOfNulls<ValueType>(UByte.MAX_VALUE.toInt()).apply {
            ValueType.values().forEach { this[it.codec.type.toInt()] = it }
        }

        fun forId(id: UByte): ValueType = MAPPING[id.toInt()] ?: UNKNOWN
    }
}

interface ValueCodec<T> {
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

abstract class ListCodec<T>(type: UByte, private val elementCodec: ValueCodec<T>) : AbstractCodec<List<T>>(type) {
    override fun read(buffer: ByteBuf): List<T> = mutableListOf<T>().also { list ->
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
    override fun read(buffer: ByteBuf): Map<K, V> = mutableMapOf<K, V>().apply {
        while (buffer.isReadable) {
            this[keyCodec.decode(buffer)] = valueCodec.decode(buffer)
        }
    }

    override fun write(buffer: ByteBuf, value: Map<K, V>): Unit = value.forEach { (key, value) ->
        keyCodec.encode(key, buffer)
        valueCodec.encode(value, buffer)
    }
}

abstract class ByteArrayCodec(type: UByte) : AbstractCodec<ByteArray>(type) {
    override fun read(buffer: ByteBuf): ByteArray = ByteArray(buffer.readableBytes()).apply(buffer::readBytes)

    override fun write(buffer: ByteBuf, value: ByteArray) {
        buffer.writeBytes(value)
    }
}

abstract class InstantCodec(type: UByte) : AbstractCodec<Instant>(type) {
    override fun read(buffer: ByteBuf): Instant = Instant.ofEpochSecond(buffer.readLongLE(), buffer.readIntLE().toLong())

    override fun write(buffer: ByteBuf, value: Instant) {
        buffer.writeLongLE(value.epochSecond).writeIntLE(value.nano)
    }
}

open class CborCodec<T>(type: UByte, private val typeReference: TypeReference<T>) : AbstractCodec<T>(type) {
    override fun read(buffer: ByteBuf): T = ByteBufInputStream(buffer).use {
        return MAPPER.readValue(it as InputStream, typeReference)
    }

    override fun write(buffer: ByteBuf, value: T) = ByteBufOutputStream(buffer).use {
        MAPPER.writeValue(it as OutputStream, value)
    }

    companion object {
        private val MAPPER = CBORMapper().registerModule(JavaTimeModule())
    }
}

object LongTypeCodec : LongCodec(1u)

object StringTypeCodec : StringCodec(2u)

object MessageIdCodec : AbstractCodec<DemoMessageId>(10u) {
    override fun read(buffer: ByteBuf): DemoMessageId = DemoMessageId().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is SessionAliasCodec -> sessionAlias = codec.decode(buffer)
                is DirectionCodec -> direction = codec.decode(buffer)
                is SequenceCodec -> sequence = codec.decode(buffer)
                is SubsequenceCodec -> subsequence = codec.decode(buffer)
                is TimestampCodec -> timestamp = codec.decode(buffer)
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }

    override fun write(buffer: ByteBuf, value: DemoMessageId) {
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

object DirectionCodec : AbstractCodec<DemoDirection>(104u) {
    override fun read(buffer: ByteBuf): DemoDirection = DemoDirection.forId(buffer.readByte().toInt())

    override fun write(buffer: ByteBuf, value: DemoDirection) {
        buffer.writeByte(value.id)
    }
}

object SequenceCodec : LongCodec(105u)

object SubsequenceCodec : ListCodec<Long>(106u, LongTypeCodec)

object TimestampCodec : InstantCodec(107u)

object MetadataCodec : MapCodec<String, String>(11u, StringTypeCodec, StringTypeCodec)

object ProtocolCodec : StringCodec(12u)

object RawMessageCodec : AbstractCodec<DemoRawMessage>(20u) {
    override fun read(buffer: ByteBuf): DemoRawMessage = DemoRawMessage().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is MessageIdCodec -> id = codec.decode(buffer)
                is MetadataCodec -> metadata = codec.decode(buffer)
                is ProtocolCodec -> protocol = codec.decode(buffer)
                is RawMessageBodyCodec -> body = codec.decode(buffer)
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }

    override fun write(buffer: ByteBuf, value: DemoRawMessage) {
        MessageIdCodec.encode(value.id, buffer)
        MetadataCodec.encode(value.metadata, buffer)
        ProtocolCodec.encode(value.protocol, buffer)
        RawMessageBodyCodec.encode(value.body, buffer)
    }
}

object RawMessageBodyCodec : ByteArrayCodec(21u)

object ParsedMessageCodec : CborCodec<DemoParsedMessage>(30u, jacksonTypeRef())

object MessageGroupCodec : AbstractCodec<DemoMessageGroup>(40u) {
    override fun read(buffer: ByteBuf): DemoMessageGroup = DemoMessageGroup().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is MessageListCodec -> messages = codec.decode(buffer)
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }

    override fun write(buffer: ByteBuf, value: DemoMessageGroup) {
        MessageListCodec.encode(value.messages, buffer)
    }
}

object MessageListCodec : AbstractCodec<List<DemoMessage<*>>>(41u) {
    override fun read(buffer: ByteBuf): List<DemoMessage<*>> = mutableListOf<DemoMessage<*>>().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is RawMessageCodec -> this += codec.decode(buffer)
                is ParsedMessageCodec -> this += codec.decode(buffer)
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }
    }

    override fun write(buffer: ByteBuf, value: List<DemoMessage<*>>): Unit = value.forEach { message ->
        when (message) {
            is DemoRawMessage -> RawMessageCodec.encode(message, buffer)
            is DemoParsedMessage -> ParsedMessageCodec.encode(message, buffer)
            else -> println("Skipping unsupported message type: $message")
        }
    }
}

object GroupBatchCodec : AbstractCodec<DemoGroupBatch>(50u) {
    override fun read(buffer: ByteBuf): DemoGroupBatch = DemoGroupBatch().apply {
        buffer.forEachValue { codec ->
            when (codec) {
                is BookCodec -> book = codec.decode(buffer)
                is SessionGroupCodec -> sessionGroup = codec.decode(buffer)
                is GroupListCodec -> groups = codec.decode(buffer)
                else -> println("Skipping unexpected type ${codec.type} value: ${codec.decode(buffer)}")
            }
        }

        groups.forEach {
            it.messages.forEach {
                val id = it.id
                id.book = book
                id.sessionGroup = sessionGroup
            }
        }
    }

    override fun write(buffer: ByteBuf, value: DemoGroupBatch) {
        BookCodec.encode(value.book, buffer)
        SessionGroupCodec.encode(value.sessionGroup, buffer)
        GroupListCodec.encode(value.groups, buffer)
    }
}

object GroupListCodec : ListCodec<DemoMessageGroup>(51u, MessageGroupCodec)

fun ByteBuf.forEachValue(action: (codec: ValueCodec<*>) -> Unit) {
    while (isReadable) {
        val type = getByte(readerIndex()).toUByte()

        when (val codec = ValueType.forId(type).codec) {
            is UnknownValueCodec -> println("Skipping unknown type $type value: ${ByteBufUtil.hexDump(codec.decode(this))}")
            else -> action(codec)
        }
    }
}

enum class DemoDirection(val id: Int) {
    INCOMING(1),
    OUTGOING(2);

    companion object {
        fun forId(id: Int): DemoDirection = when (id) {
            1 -> INCOMING
            2 -> OUTGOING
            else -> error("Unknown direction id: $id")
        }
    }
}

data class DemoMessageId(
    var book: String = "",
    var sessionGroup: String = "",
    var sessionAlias: String = "",
    var direction: DemoDirection = INCOMING,
    var sequence: Long = 0,
    var subsequence: List<Long> = listOf(),
    var timestamp: Instant = Instant.EPOCH,
) {
    companion object {
        val DEFAULT_INSTANCE: DemoMessageId = DemoMessageId() // FIXME: do smth about its mutability
    }
}

interface DemoMessage<T> {
    var id: DemoMessageId
    var metadata: Map<String, String>
    var protocol: String
    var body: T
}

data class DemoRawMessage(
    override var id: DemoMessageId = DemoMessageId.DEFAULT_INSTANCE,
    override var metadata: Map<String, String> = mapOf(),
    override var protocol: String = "",
    override var body: ByteArray = EMPTY_BODY,
) : DemoMessage<ByteArray> {
    override fun equals(other: Any?): Boolean = when {
        this === other -> true
        other !is DemoRawMessage -> false
        id != other.id -> false
        metadata != other.metadata -> false
        protocol != other.protocol -> false
        else -> body.contentEquals(other.body)
    }

    override fun hashCode(): Int = Objects.hash(id, metadata, protocol, body.contentHashCode())

    companion object {
        private val EMPTY_BODY = ByteArray(0)
    }
}

data class DemoParsedMessage(
    override var id: DemoMessageId = DemoMessageId.DEFAULT_INSTANCE,
    override var metadata: Map<String, String> = mapOf(),
    override var protocol: String = "",
    var type: String = "",
    override var body: Map<String, Any> = mapOf(),
) : DemoMessage<Map<String, Any>>

data class DemoMessageGroup(
    var messages: List<DemoMessage<*>> = listOf(),
)

data class DemoGroupBatch(
    var book: String = "",
    var sessionGroup: String = "",
    var groups: List<DemoMessageGroup> = listOf(),
)

fun DemoGroupBatch.toByteArray() = Unpooled.buffer().run {
    GroupBatchCodec.encode(this@toByteArray, this@run)
    ByteArray(readableBytes()).apply(::readBytes)
}

fun main() {
    val buffer = Unpooled.buffer()

    val message1 = DemoRawMessage(
        id = DemoMessageId(
            book = "book1",
            sessionGroup = "group1",
            sessionAlias = "alias1",
            direction = INCOMING,
            sequence = 1,
            subsequence = listOf(1, 2),
            timestamp = Instant.now()
        ),
        metadata = mapOf(
            "prop1" to "value1",
            "prop2" to "value2"
        ),
        protocol = "proto1",
        body = byteArrayOf(1, 2, 3, 4)
    )

    val message2 = DemoRawMessage(
        id = DemoMessageId(
            book = "book1",
            sessionGroup = "group1",
            sessionAlias = "alias2",
            direction = OUTGOING,
            sequence = 2,
            subsequence = listOf(3, 4),
            timestamp = Instant.now()
        ),
        metadata = mapOf(
            "prop3" to "value3",
            "prop4" to "value4"
        ),
        protocol = "proto2",
        body = byteArrayOf(5, 6, 7, 8)
    )

    val message3 = DemoParsedMessage(
        id = DemoMessageId(
            book = "book1",
            sessionGroup = "group1",
            sessionAlias = "alias3",
            direction = OUTGOING,
            sequence = 3,
            subsequence = listOf(5, 6),
            timestamp = Instant.now()
        ),
        metadata = mapOf(
            "prop5" to "value6",
            "prop7" to "value8"
        ),
        protocol = "proto3",
        type = "some-type",
        body = mapOf(
            "simple" to 1,
            "list" to listOf(1, 2, 3),
            "map" to mapOf("abc" to "cde")
        )
    )

    val batch = DemoGroupBatch(
        book = "book1",
        sessionGroup = "group1",
        groups = listOf(DemoMessageGroup(listOf(message1, message2, message3)))
    )

    GroupBatchCodec.encode(batch, buffer)

    val decodedBatch = GroupBatchCodec.decode(buffer)

    println(batch)
    println(decodedBatch)
    println(batch == decodedBatch)
    println(buffer)
}
