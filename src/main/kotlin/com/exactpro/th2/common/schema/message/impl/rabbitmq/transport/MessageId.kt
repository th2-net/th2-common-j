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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.CollectionBuilder
import com.google.auto.value.AutoBuilder
import java.time.Instant

data class MessageId(
    val sessionAlias: String,
    val direction: Direction,
    val sequence: Long,
    val timestamp: Instant,
    /** The subsequence is not mutable by default */
    val subsequence: List<Int> = emptyList(),
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MessageId

        if (sessionAlias != other.sessionAlias) return false
        if (direction != other.direction) return false
        if (sequence != other.sequence) return false
        if (timestamp != other.timestamp) return false
        return subsequence == other.subsequence
    }

    override fun hashCode(): Int {
        var result = sessionAlias.hashCode()
        result = 31 * result + direction.hashCode()
        result = 31 * result + sequence.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + subsequence.hashCode()
        return result
    }

    override fun toString(): String {
        return "MessageId(sessionAlias='$sessionAlias', direction=$direction, sequence=$sequence, timestamp=$timestamp, subsequence=$subsequence)"
    }

    @AutoBuilder
    interface Builder {
        val sessionAlias: String
        val direction: Direction
        val sequence: Long
        val timestamp: Instant

        fun setSessionAlias(sessionAlias: String): Builder
        fun setDirection(direction: Direction): Builder
        fun setSequence(sequence: Long): Builder
        fun setTimestamp(timestamp: Instant): Builder
        fun subsequenceBuilder(): CollectionBuilder<Int>
        fun addSubsequence(subsequence: Int): Builder = apply {
            subsequenceBuilder().add(subsequence)
        }

        fun setSubsequence(subsequence: List<Int>): Builder
        fun build(): MessageId
    }

    fun toBuilder(): Builder = AutoBuilder_MessageId_Builder(this)

    companion object {
        @JvmStatic
        val DEFAULT: MessageId = MessageId("", Direction.OUTGOING, 0, Instant.EPOCH)

        @JvmStatic
        fun builder(): Builder = AutoBuilder_MessageId_Builder()
    }
}