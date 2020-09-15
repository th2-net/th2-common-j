/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
 *****************************************************************************/

/*******************************************************************************
 *  Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.common.message

import com.exactpro.th2.common.value.getBigDecimal
import com.exactpro.th2.common.value.getBigInteger
import com.exactpro.th2.common.value.getDouble
import com.exactpro.th2.common.value.getInt
import com.exactpro.th2.common.value.getLong
import com.exactpro.th2.common.value.getMessage
import com.exactpro.th2.common.value.getString
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.infra.grpc.ConnectionID
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.infra.grpc.MessageMetadata
import com.exactpro.th2.infra.grpc.Value
import com.google.protobuf.Timestamp
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import kotlin.DeprecationLevel.ERROR

fun message() : Message.Builder = Message.newBuilder()
fun message(messageType: String): Message.Builder = Message.newBuilder().setMetadata(messageType)
fun message(messageType: String, direction: Direction, sessionAlias: String): Message.Builder = Message.newBuilder().setMetadata(messageType, direction, sessionAlias)

fun Message.getField(fieldName: String): Value? = getFieldsOrDefault(fieldName, null)
fun Message.Builder.getField(fieldName: String): Value? = getFieldsOrDefault(fieldName, null)

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

fun Message.Builder.addField(key: String, value: Any?): Message.Builder = apply { putFields(key, value?.toValue() ?: nullValue()) }

@Deprecated(
    message = "Will be renamed",
    level = ERROR,
    replaceWith = ReplaceWith(
        expression = "this.copyField(message, key)",
        imports = []
    ))
fun Message.Builder.addField(message: Message, key: String) : Message.Builder = apply { if (message.getField(key) != null) putFields(key, message.getField(key)) }
@Deprecated(
    message = "Will be renamed",
    level = ERROR,
    replaceWith = ReplaceWith(
        expression = "this.copyField(message, key)",
        imports = []
    ))
fun Message.Builder.addField(message: Message.Builder, key: String): Message.Builder = apply { if (message.getField(key) != null) putFields(key, message.getField(key)) }

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

@Deprecated(
    message = "Will be renamed",
    level = ERROR,
    replaceWith = ReplaceWith(
        expression = "this.copyFields(message, *keys)",
        imports = []
    ))
fun Message.Builder.addFields(message: Message, vararg keys: String) : Message.Builder = apply { keys.forEach { copyField(message, it) } }
@Deprecated(
    message = "Will be renamed",
    level = ERROR,
    replaceWith = ReplaceWith(
        expression = "this.copyFields(message, *keys)",
        imports = []
    ))
fun Message.Builder.addFields(message: Message.Builder, vararg keys: String) : Message.Builder = apply { keys.forEach { copyField(message, it) } }

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

fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder().setSeconds(epochSecond).setNanos(nano).build()
fun Date.toTimestamp(): Timestamp = toInstant().toTimestamp()
fun LocalDateTime.toTimestamp(zone: ZoneOffset) : Timestamp = toInstant(zone).toTimestamp()
fun LocalDateTime.toTimestamp() : Timestamp = toTimestamp(ZoneOffset.of(TimeZone.getDefault().id))
fun Calendar.toTimestamp() : Timestamp = toInstant().toTimestamp()