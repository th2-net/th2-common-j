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

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.Value.KindCase.LIST_VALUE
import com.exactpro.th2.common.grpc.Value.KindCase.MESSAGE_VALUE
import com.exactpro.th2.common.grpc.Value.KindCase.NULL_VALUE
import com.exactpro.th2.common.grpc.Value.KindCase.SIMPLE_VALUE
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.BOOK_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.DIRECTION_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.MESSAGE_TYPE_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.SESSION_ALIAS_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.AbstractTh2MsgFilterStrategy.SESSION_GROUP_KEY
import com.exactpro.th2.common.schema.filter.strategy.impl.checkFieldValue
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.value.toValue
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.Message as ProtoMessage

fun GroupBatch.toByteArray(): ByteArray = Unpooled.buffer().run {
    GroupBatchCodec.encode(this@toByteArray, this@run)
    ByteArray(readableBytes()).apply(::readBytes)
}

fun ByteBuf.toByteArray(): ByteArray = ByteArray(readableBytes())
    .apply(::readBytes).also { resetReaderIndex() }

fun Collection<RouterFilter>.filter(batch: GroupBatch): GroupBatch? {
    if (isEmpty()) {
        return batch
    }

    forEach { filterSet ->
        if (!filterSet.metadata[BOOK_KEY].verify(batch.book)) {
            return@forEach
        }
        if (!filterSet.metadata[SESSION_GROUP_KEY].verify(batch.sessionGroup)) {
            return@forEach
        }

        if (!filterSet.metadata[SESSION_ALIAS_KEY].verify(batch.groups) { id.sessionAlias }) {
            return@forEach
        }
        if (!filterSet.metadata[MESSAGE_TYPE_KEY].verify(batch.groups) { if (this is ParsedMessage) type else "" }) {
            return@forEach
        }
        if (!filterSet.metadata[DIRECTION_KEY].verify(batch.groups) { id.direction.proto.name }) {
            return@forEach
        }

        return batch
    }

    return null
}

val Direction.proto: ProtoDirection
    get() = when (this) {
        INCOMING -> FIRST
        OUTGOING -> SECOND
    }

fun EventId.toProto(): EventID = EventID.newBuilder().also {
    it.id = id
    it.bookName = book
    it.scope = scope
    it.startTimestamp = timestamp.toTimestamp()
}.build()

fun MessageId.toProto(book: String, sessionGroup: String): MessageID = MessageID.newBuilder().also {
    it.bookName = book
    it.direction = if (direction == INCOMING) FIRST else SECOND
    it.sequence = sequence
    it.timestamp = timestamp.toTimestamp()

    it.addAllSubsequence(subsequence)

    it.connectionIdBuilder.also { connectionId ->
        connectionId.sessionGroup = sessionGroup.ifBlank { sessionAlias }
        connectionId.sessionAlias = sessionAlias
    }
}.build()

fun MessageId.toProto(groupBatch: GroupBatch): MessageID = toProto(groupBatch.book, groupBatch.sessionGroup)
val ProtoDirection.transport: Direction
    get() = when (this) {
        FIRST -> INCOMING
        SECOND -> OUTGOING
        else -> error("Unsupported $this direction in the th2 transport protocol")
    }

fun ParsedMessage.toProto(book: String, sessionGroup: String): ProtoMessage = ProtoMessage.newBuilder().apply {
    metadataBuilder.apply {
        id = this@toProto.id.toProto(book, sessionGroup)
        messageType = this@toProto.type
        protocol = this@toProto.protocol
        putAllProperties(this@toProto.metadata)
    }
    body.forEach { (key, value) -> addField(key, value.toValue()) }
    eventId?.let { parentEventId = eventId.toProto() }
}.build()

fun EventID.toTransport(): EventId = EventId(id, bookName, scope, startTimestamp.toInstant())
fun MessageID.toTransport(): MessageId =
    MessageId(connectionId.sessionAlias, direction.transport, sequence, timestamp.toInstant(), subsequenceList)

fun Value.toTransport(): Any? = when (kindCase) {
    NULL_VALUE -> null
    SIMPLE_VALUE -> simpleValue
    MESSAGE_VALUE -> messageValue.fieldsMap.mapValues { entry -> entry.value.toTransport() }
    LIST_VALUE -> listValue.valuesList.map(Value::toTransport)
    else -> "Unsupported $kindCase kind for transformation, value: ${toJson()}"
}

fun ProtoMessage.toTransport(): ParsedMessage = ParsedMessage.builder().apply {
    with(metadata) {
        setId(id.toTransport())
        setType(messageType)
        setProtocol(protocol)
        setMetadata(propertiesMap)
    }
    with(bodyBuilder()) {
        fieldsMap.forEach { (key, value) ->
            put(key, value.toTransport())
        }
    }
    if (hasParentEventId()) {
        setEventId(parentEventId.toTransport())
    }
}.build()

private fun Collection<FieldFilterConfiguration>?.verify(value: String): Boolean {
    if (isNullOrEmpty()) {
        return true
    }
    return all { it.checkFieldValue(value) }
}

private inline fun Collection<FieldFilterConfiguration>?.verify(
    messageGroups: Collection<MessageGroup>,
    value: Message<*>.() -> String
): Boolean {
    if (isNullOrEmpty()) {
        return true
    }

    // Illegal cases when groups or messages are empty
    if (messageGroups.isEmpty()) {
        return false
    }
    val firstGroup = messageGroups.first()
    if (firstGroup.messages.isEmpty()) {
        return false
    }

    return all { filter -> filter.checkFieldValue(firstGroup.messages.first().value()) }
}