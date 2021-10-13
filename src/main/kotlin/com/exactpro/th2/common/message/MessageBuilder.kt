/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.message

import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.google.protobuf.Timestamp
import java.time.Instant

abstract class MessageBuilder<T> {
    private lateinit var sessionAlias: String
    protected var directionValue: Int = Direction.SECOND_VALUE
    protected var sequence: Long = -1
    protected var subsequences = mutableListOf<Int>()
    protected var timestamp: Instant = Instant.now()
    protected var properties = mutableMapOf<String, String>()
    protected var protocol: String = ""

    abstract fun toProto(parentEventId: EventID?): T

    fun getMessageId(): MessageID {
        return MessageID.newBuilder()
            .setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias))
            .setDirectionValue(directionValue)
            .setSequence(sequence)
            .addAllSubsequence(subsequences)
            .build()
    }

    fun getTimestamp(): Timestamp {
        return Timestamp.newBuilder()
            .setSeconds(timestamp.epochSecond)
            .setNanos(timestamp.nano)
            .build()
    }

    fun sessionAlias(sessionAlias: String): MessageBuilder<T> {
        this.sessionAlias = sessionAlias
        return this
    }

    fun direction(direction: Int): MessageBuilder<T> {
        this.directionValue = direction
        return this
    }

    fun sequence(sequence: Long): MessageBuilder<T> {
        this.sequence = sequence
        return this
    }

    fun addSubsequence(subsequence: Int): MessageBuilder<T> {
        subsequences.add(subsequence)
        return this
    }

    fun timestamp(timestamp: Instant): MessageBuilder<T> {
        this.timestamp = timestamp
        return this
    }

    fun addProperty(propertyKey: String, propertyValue: String): MessageBuilder<T> {
        properties[propertyKey] = propertyValue
        return this
    }

    fun protocol(protocol: String): MessageBuilder<T> {
        this.protocol = protocol
        return this
    }

    open fun messageType(messageType: String): MessageBuilder<T> {
        return this
    }

    open fun addNullField(field: String): MessageBuilder<T> {
        return this
    }

    open fun addSimpleField(field: String, value: String): MessageBuilder<T> {
        return this
    }

    open fun addSimpleListField(field: String, values: List<String>): MessageBuilder<T> {
        return this
    }

    open fun addMessageField(field: String, message: T): MessageBuilder<T> {
        return this
    }

    open fun addMessageListField(field: String, messages: List<T>): MessageBuilder<T> {
        return this
    }

    open fun bytes(bytes: ByteArray): MessageBuilder<T> {
        return this
    }
}