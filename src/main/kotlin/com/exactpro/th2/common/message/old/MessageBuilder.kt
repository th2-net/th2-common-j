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

package com.exactpro.th2.common.message.old

import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.google.protobuf.Timestamp
import java.time.Instant

abstract class MessageBuilder<T : MessageBuilder<T>> {
    private lateinit var sessionAlias: String
    protected var directionValue: Int = Direction.SECOND_VALUE
    protected var sequence: Long = -1
    protected var subsequences = mutableListOf<Int>()
    protected var timestamp: Instant = Instant.now()
    protected var properties = mutableMapOf<String, String>()
    protected var protocol: String = ""

    protected abstract fun builder(): T

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

    fun sessionAlias(sessionAlias: String): T {
        this.sessionAlias = sessionAlias
        return builder()
    }

    fun direction(direction: Int): T {
        this.directionValue = direction
        return builder()
    }

    fun sequence(sequence: Long): T {
        this.sequence = sequence
        return builder()
    }

    fun addSubsequence(subsequence: Int): T {
        subsequences.add(subsequence)
        return builder()
    }

    fun timestamp(timestamp: Instant): T {
        this.timestamp = timestamp
        return builder()
    }

    fun addProperty(propertyKey: String, propertyValue: String): T {
        properties[propertyKey] = propertyValue
        return builder()
    }

    fun protocol(protocol: String): T {
        this.protocol = protocol
        return builder()
    }
}