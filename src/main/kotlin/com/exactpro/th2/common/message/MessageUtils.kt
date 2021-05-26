/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("MessageUtils")

package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.ListValueOrBuilder
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MessageOrBuilder
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueOrBuilder
import com.exactpro.th2.common.value.getBigDecimal
import com.exactpro.th2.common.value.getBigInteger
import com.exactpro.th2.common.value.getDouble
import com.exactpro.th2.common.value.getInt
import com.exactpro.th2.common.value.getLong
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.common.value.updateList
import com.exactpro.th2.common.value.updateMessage
import com.exactpro.th2.common.value.updateOrAddList
import com.exactpro.th2.common.value.updateOrAddMessage
import com.exactpro.th2.common.value.updateOrAddString
import com.exactpro.th2.common.value.updateString
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Calendar
import java.util.Date
import java.util.TimeZone

fun message() : Message.Builder = Message.newBuilder()
fun message(messageType: String): Message.Builder = Message.newBuilder().setMetadata(messageType)
fun message(messageType: String, direction: Direction, sessionAlias: String): Message.Builder = Message.newBuilder().setMetadata(messageType, direction, sessionAlias)

operator fun Message.get(key: String): Value? = getField(key)
fun Message.getField(fieldName: String): Value? = getFieldsOrDefault(fieldName, null)
operator fun Message.Builder.get(key: String): Value? = getField(key)
fun Message.Builder.getField(fieldName: String): Value? = getFieldsOrDefault(fieldName, null)

fun Message.hasField(key: String) : Boolean = fieldsMap.containsKey(key)
fun Message.Builder.hasField(key: String) : Boolean = fieldsMap.containsKey(key);

fun Message.getString(fieldName: String): String? = getField(fieldName)?.getString()
fun Message.getInt(fieldName: String): Int? = getField(fieldName)?.getInt()
fun Message.getLong(fieldName: String): Long? = getField(fieldName)?.getLong()
fun Message.getDouble(fieldName: String): Double? = getField(fieldName)?.getDouble()
fun Message.getBigInteger(fieldName: String): BigInteger? = getField(fieldName)?.getBigInteger()
fun Message.getBigDecimal(fieldName: String): BigDecimal? = getField(fieldName)?.getBigDecimal()
fun Message.getMessage(fieldName: String): Message? = getField(fieldName)?.getMessage()
fun Message.getList(fieldName: String): List<Value>? = getField(fieldName)?.listValue?.valuesList

fun Message.Builder.getString(fieldName: String): String? = getField(fieldName)?.getString()
fun Message.Builder.getInt(fieldName: String): Int? = getField(fieldName)?.getInt()
fun Message.Builder.getLong(fieldName: String): Long? = getField(fieldName)?.getLong()
fun Message.Builder.getDouble(fieldName: String): Double? = getField(fieldName)?.getDouble()
fun Message.Builder.getBigInteger(fieldName: String): BigInteger? = getField(fieldName)?.getBigInteger()
fun Message.Builder.getBigDecimal(fieldName: String): BigDecimal? = getField(fieldName)?.getBigDecimal()
fun Message.Builder.getMessage(fieldName: String): Message? = getField(fieldName)?.getMessage()
fun Message.Builder.getList(fieldName: String): List<Value>? = getField(fieldName)?.listValue?.valuesList


operator fun Message.Builder.set(key: String, value: Any?): Message.Builder = apply { addField(key, value) }
fun Message.Builder.updateField(key: String, updateFunc: Value.Builder.() -> ValueOrBuilder?): Message.Builder = apply { set(key, updateFunc(getField(key)?.toBuilder() ?: throw NullPointerException("Can not find field with name $key"))) }
fun Message.Builder.updateList(key: String, updateFunc: ListValue.Builder.() -> ListValueOrBuilder) : Message.Builder = apply { updateField(key) { updateList(updateFunc) } }
fun Message.Builder.updateMessage(key: String, updateFunc: Message.Builder.() -> MessageOrBuilder) : Message.Builder = apply { updateField(key) { updateMessage(updateFunc) } }
fun Message.Builder.updateString(key: String, updateFunc: String.() -> String) : Message.Builder = apply { updateField(key) { updateString(updateFunc) } }

fun Message.Builder.updateOrAddField(key: String, updateFunc: (Value.Builder?) -> ValueOrBuilder?): Message.Builder = apply { set(key, updateFunc(getField(key)?.toBuilder())) }
fun Message.Builder.updateOrAddList(key: String, updateFunc: (ListValue.Builder?) -> ListValueOrBuilder) : Message.Builder = apply { updateOrAddField(key) { it?.updateOrAddList(updateFunc) ?: updateFunc(null)?.toValue() } }
fun Message.Builder.updateOrAddMessage(key: String, updateFunc: (Message.Builder?) -> MessageOrBuilder) : Message.Builder = apply { updateOrAddField(key) { it?.updateOrAddMessage(updateFunc) ?: updateFunc(null)?.toValue() } }
fun Message.Builder.updateOrAddString(key: String, updateFunc:(String?) -> String) : Message.Builder = apply { updateOrAddField(key) { it?.updateOrAddString(updateFunc) ?: updateFunc(null)?.toValue() } }

fun Message.Builder.addField(key: String, value: Any?): Message.Builder = apply { putFields(key, value?.toValue() ?: nullValue()) }

fun Message.Builder.copyField(message: Message, key: String) : Message.Builder = apply { if (message.getField(key) != null) putFields(key, message.getField(key)) }
fun Message.Builder.copyField(message: Message.Builder, key: String): Message.Builder = apply { if (message.getField(key) != null) putFields(key, message.getField(key)) }


/**
 * Accepts vararg with even size. It split to pair: the first value is used as key, the second value is used as value
 */
fun Message.Builder.addFields(vararg fields: Any?): Message.Builder = apply {
    for (i in fields.indices step 2) {
        addField(fields[i] as String, fields[i + 1])
    }
}

fun Message.Builder.addFields(fields: Map<String, Any?>?): Message.Builder = apply { fields?.forEach { addField(it.key, it.value?.toValue() ?: nullValue()) } }

fun Message.Builder.copyFields(message: Message, vararg keys: String) : Message.Builder = apply { keys.forEach { copyField(message, it) } }
fun Message.Builder.copyFields(message: Message.Builder, vararg keys: String) : Message.Builder = apply { keys.forEach { copyField(message, it) } }

fun Message.copy(): Message.Builder = Message.newBuilder().setMetadata(metadata).putAllFields(fieldsMap).setParentEventId(parentEventId)

fun Message.Builder.copy(): Message.Builder = Message.newBuilder().setMetadata(metadata).putAllFields(fieldsMap).setParentEventId(parentEventId)

fun Message.Builder.setMetadata(messageType: String? = null, direction: Direction? = null, sessionAlias: String? = null, sequence: Long? = null, timestamp: Instant? = null): Message.Builder =
    setMetadata(MessageMetadata.newBuilder().also {
        if (messageType != null) {
            it.messageType = messageType
        }
        it.timestamp = (timestamp ?: Instant.now()).toTimestamp()
        if (direction != null || sessionAlias != null) {
            it.id = MessageID.newBuilder().apply {
                if (direction != null) {
                    this.direction = direction
                }
                if (sessionAlias != null) {
                    connectionId = ConnectionID.newBuilder().setSessionAlias(sessionAlias).build()
                }
                if (sequence != null) {
                    this.sequence = sequence
                }
            }.build()
        }
    })

operator fun MessageGroup.Builder.plusAssign(message: Message) {
    addMessages(AnyMessage.newBuilder().setMessage(message))
}

operator fun MessageGroup.Builder.plusAssign(message: Message.Builder) {
    addMessages(AnyMessage.newBuilder().setMessage(message))
}

operator fun MessageGroup.Builder.plusAssign(rawMessage: RawMessage) {
    addMessages(AnyMessage.newBuilder().setRawMessage(rawMessage))
}

operator fun MessageGroup.Builder.plusAssign(rawMessage: RawMessage.Builder) {
    addMessages(AnyMessage.newBuilder().setRawMessage(rawMessage))
}

fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder().setSeconds(epochSecond).setNanos(nano).build()
fun Date.toTimestamp(): Timestamp = toInstant().toTimestamp()
fun LocalDateTime.toTimestamp(zone: ZoneOffset) : Timestamp = toInstant(zone).toTimestamp()
fun LocalDateTime.toTimestamp() : Timestamp = toTimestamp(ZoneOffset.of(TimeZone.getDefault().id))
fun Calendar.toTimestamp() : Timestamp = toInstant().toTimestamp()

val Message.messageType
    get(): String = metadata.messageType
var Message.Builder.messageType
    get(): String = metadata.messageType
    set(value) {
        setMetadata(MessageMetadata.newBuilder(metadata).apply {
            messageType = value
        })
    }

val Message.direction
    get(): Direction = metadata.id.direction
var Message.Builder.direction
    get(): Direction = metadata.id.direction
    set(value) {
        setMetadata(MessageMetadata.newBuilder(metadata).apply {
            setId(MessageID.newBuilder(id).apply {
                direction = value
            })
        })
    }

val Message.sessionAlias
    get(): String = metadata.id.connectionId.sessionAlias
var Message.Builder.sessionAlias
    get(): String = metadata.id.connectionId.sessionAlias
    set(value) {
        setMetadata(MessageMetadata.newBuilder(metadata).apply {
            setId(MessageID.newBuilder(id).apply {
                setConnectionId(ConnectionID.newBuilder(connectionId).apply {
                    sessionAlias = value
                })
            })
        })
    }

val Message.sequence
    get(): Long = metadata.id.sequence
var Message.Builder.sequence
    get(): Long = metadata.id.sequence
    set(value) {
        setMetadata(MessageMetadata.newBuilder(metadata).apply {
            setId(MessageID.newBuilder(id).apply {
                sequence = value
            })
        })
    }

fun getSessionAliasAndDirection(messageID: MessageID): Array<String> = arrayOf(messageID.connectionId.sessionAlias, messageID.direction.name)

private val unknownLabels = arrayOf("unknown", "unknown")

fun getSessionAliasAndDirection(anyMessage: AnyMessage): Array<String> = when {
    anyMessage.hasMessage() -> getSessionAliasAndDirection(anyMessage.message.metadata.id)
    anyMessage.hasRawMessage() -> getSessionAliasAndDirection(anyMessage.rawMessage.metadata.id)
    else -> unknownLabels
}

fun getDebugString(className: String, ids: List<MessageID>): String {
    val sessionAliasAndDirection = getSessionAliasAndDirection(ids[0])
    val sequences = ids.joinToString { it.sequence.toString() }
    return "$className: session_alias = ${sessionAliasAndDirection[0]}, direction = ${sessionAliasAndDirection[1]}, sequnces = $sequences"
}

@JvmOverloads
fun com.google.protobuf.MessageOrBuilder.toJson(short: Boolean = true): String = JsonFormat.printer().includingDefaultValueFields().let {
    (if (short) it.omittingInsignificantWhitespace() else it).print(this)
}

fun <T: com.google.protobuf.Message.Builder> T.fromJson(json: String) : T = apply {
    JsonFormat.parser().ignoringUnknownFields().merge(json, this)
}