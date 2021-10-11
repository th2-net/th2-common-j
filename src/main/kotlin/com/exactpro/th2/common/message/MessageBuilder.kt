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
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.google.protobuf.Timestamp
import java.time.Instant

abstract class MessageBuilder<T> {
    private var sessionAlias: String? = null
    private var direction: Int? = null
    private var sequence: Long? = null
    private var subsequences: List<Int>? = null
    private var timestamp: Instant? = null
    protected var properties: Map<String, String>? = null
    protected var protocol: String? = null

    abstract fun toProto(parentEventId: EventID): T

    fun getMessageId(): MessageID {
        val idBuilder = MessageID.newBuilder()
        if (sessionAlias != null) {
            idBuilder.setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias))
        }
        if (direction != null) {
            idBuilder.directionValue = direction!!
        }
        if (sequence != null) {
            idBuilder.sequence = sequence!!
        }
        if (subsequences != null) {
            idBuilder.addAllSubsequence(subsequences!!)
        }
        return idBuilder.build()
    }

    fun getTimestamp(): Timestamp? {
        return if (timestamp == null) {
            null
        } else Timestamp.newBuilder()
            .setSeconds(timestamp!!.epochSecond)
            .setNanos(timestamp!!.nano)
            .build()
    }

    fun sessionAlias(sessionAlias: String): MessageBuilder<T> {
        this.sessionAlias = sessionAlias
        return this
    }

    fun direction(direction: Int): MessageBuilder<T> {
        this.direction = direction
        return this
    }

    fun sequence(sequence: Long): MessageBuilder<T> {
        this.sequence = sequence
        return this
    }

    fun subsequences(subsequences: List<Int>): MessageBuilder<T> {
        this.subsequences = subsequences
        return this
    }

    fun timestamp(timestamp: Instant): MessageBuilder<T> {
        this.timestamp = timestamp
        return this
    }

    fun properties(properties: Map<String, String>): MessageBuilder<T> {
        this.properties = properties
        return this
    }

    fun protocol(protocol: String): MessageBuilder<T> {
        this.protocol = protocol
        return this
    }

    private companion object {
        @JvmStatic
        fun startParsedBuilder() = ParsedMessageBuilder()

        @JvmStatic
        fun startRawBuilder() = RawMessageBuilder()
    }
}