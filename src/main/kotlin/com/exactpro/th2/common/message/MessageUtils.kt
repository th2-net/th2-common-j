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

import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.infra.grpc.ConnectionID
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.infra.grpc.MessageMetadata
import com.exactpro.th2.infra.grpc.Value
import com.google.protobuf.Timestamp
import java.time.Instant

fun message(messageType: String): Message.Builder = Message.newBuilder().setMetadata(messageType)
fun message(messageType: String, direction: Direction, sessionAlias: String): Message.Builder = Message.newBuilder().setMetadata(messageType, direction, sessionAlias)

fun Message.getField(key: String): Value? = getFieldsOrDefault(key, null)
fun Message.Builder.getField(key: String): Value? = getFieldsOrDefault(key, null)

fun Message.Builder.addField(key: String, value: Any?): Message.Builder = apply { putFields(key, value?.toValue() ?: nullValue()) }

fun Message.Builder.addField(message: Message, key: String) : Message.Builder = apply { if (message.getField(key) != null) putFields(key, message.getField(key)) }
fun Message.Builder.addField(message: Message.Builder, key: String): Message.Builder = apply { if (message.getField(key) != null) putFields(key, message.getField(key)) }

/**
 * Accepts vararg with even size. It split to pair: the first value is used as key, the second value is used as value
 */
fun Message.Builder.addFields(vararg fields: Any?): Message.Builder = apply {
    for (i in fields.indices step 2) {
        addField(fields[i] as String, fields[i + 1])
    }
}

fun Message.Builder.addFields(fields: Map<String, Any?>?): Message.Builder = apply { fields?.forEach { addField(it.key, it.value?.toValue() ?: nullValue()) } }

fun Message.Builder.addFields(message: Message, vararg keys: String) : Message.Builder = apply { keys.forEach { addField(message, it) } }
fun Message.Builder.addFields(message: Message.Builder, vararg keys: String) : Message.Builder = apply { keys.forEach { addField(message, it) } }

fun Message.copy(): Message.Builder = Message.newBuilder().putAllFields(fieldsMap)

fun Message.Builder.copy(): Message.Builder = Message.newBuilder().putAllFields(fieldsMap)

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