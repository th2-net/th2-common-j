/*
 * Copyright 2023-2026 Exactpro (Exactpro Systems Limited)
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
import java.time.Instant
import java.util.StringJoiner

class MessageId private constructor(
    val sessionAlias: String,
    val direction: Direction,
    val sequence: Long,
    val timestamp: Instant,
    /** The subsequence is not mutable by default */
    val subsequence: List<Int> = emptyList(),
    private val batchInfoProvider: BatchInfoProvider,
) {

    val book: String by batchInfoProvider::book
    val sessionGroup: String by batchInfoProvider::sessionGroup

    constructor(
        sessionAlias: String,
        direction: Direction,
        sequence: Long,
        timestamp: Instant,
        subsequence: List<Int> = emptyList(),
    ) : this(sessionAlias, direction, sequence, timestamp, subsequence, BatchInfoProvider.Empty)

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

    interface Builder {
        val sessionAlias: String
        fun isSessionAliasSet(): Boolean
        val direction: Direction
        fun isDirectionSet(): Boolean
        val sequence: Long
        fun isSequenceSet(): Boolean
        val timestamp: Instant
        fun isTimestampSet(): Boolean

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

    fun toBuilder(): Builder = MessageIdBuilderImpl(this)

    private class MessageIdBuilderImpl(
        private val provider: BatchInfoProvider = BatchInfoProvider.Empty,
    ) : Builder {
        private var _sessionAlias: String? = null
        private var _direction: Direction? = null
        private var _sequence: Long = SEQUENCE_NOT_SET
        private var _timestamp: Instant? = null
        private var _subsequenceBuilder: CollectionBuilder<Int>? = null
        private var _subsequence: List<Int> = emptyList()

        constructor(source: MessageId) : this(source.batchInfoProvider) {
            _sessionAlias = source.sessionAlias
            _direction = source.direction
            _sequence = source.sequence
            _timestamp = source.timestamp
            _subsequence = source.subsequence
        }

        override fun setSessionAlias(sessionAlias: String): Builder = apply {
            this._sessionAlias = sessionAlias
        }

        override val sessionAlias: String
            get() = checkNotNull(_sessionAlias) { "Property \"sessionAlias\" has not been set" }

        override fun isSessionAliasSet(): Boolean = _sessionAlias != null

        override fun setDirection(direction: Direction): Builder = apply {
            this._direction = direction
        }

        override val direction: Direction
            get() = checkNotNull(_direction) { "Property \"direction\" has not been set" }

        override fun isDirectionSet(): Boolean = _direction != null

        override fun setSequence(sequence: Long): Builder = apply {
            require(sequence != SEQUENCE_NOT_SET) { "Value $sequence for property \"sequence\" is reserved" }
            this._sequence = sequence
        }

        override val sequence: Long
            get() {
                check(_sequence != SEQUENCE_NOT_SET) { "Property \"sequence\" has not been set" }
                return _sequence
            }

        override fun isSequenceSet(): Boolean = _sequence != SEQUENCE_NOT_SET

        override fun setTimestamp(timestamp: Instant): Builder = apply {
            this._timestamp = timestamp
        }

        override val timestamp: Instant
            get() = checkNotNull(_timestamp) { "Property \"timestamp\" has not been set" }

        override fun isTimestampSet(): Boolean = _timestamp != null

        override fun setSubsequence(subsequence: List<Int>): Builder = apply {
            check(_subsequenceBuilder == null) { "Cannot set subsequence after calling subsequenceBuilder()" }
            this._subsequence = subsequence
        }

        override fun subsequenceBuilder(): CollectionBuilder<Int> {
            if (_subsequenceBuilder == null) {
                if (_subsequence.isEmpty()) {
                    _subsequenceBuilder = CollectionBuilder()
                } else {
                    _subsequenceBuilder = CollectionBuilder<Int>().apply {
                        addAll(_subsequence)
                    }
                    _subsequence = emptyList()
                }
            }
            return checkNotNull(_subsequenceBuilder) { "subsequenceBuilder" }
        }

        override fun build(): MessageId {
            _subsequence = _subsequenceBuilder?.build() ?: _subsequence
            if (_sessionAlias == null || _direction == null || _sequence == SEQUENCE_NOT_SET || _timestamp == null) {
                val missing = StringJoiner(",", "[", "]")
                if (_sessionAlias == null) {
                    missing.add("sessionAlias")
                }
                if (_direction == null) {
                    missing.add("direction")
                }
                if (_sequence == SEQUENCE_NOT_SET) {
                    missing.add("sequence")
                }
                if (_timestamp == null) {
                    missing.add("timestamp")
                }
                error("Missing required properties: $missing")
            }
            return MessageId(
                _sessionAlias!!,
                _direction!!,
                _sequence,
                _timestamp!!,
                _subsequence,
                provider,
            )
        }
    }

    companion object {
        @JvmStatic
        val DEFAULT: MessageId = MessageId("", Direction.OUTGOING, 0, Instant.EPOCH)

        @JvmStatic
        fun builder(): Builder = MessageIdBuilderImpl()

        @JvmStatic
        internal fun builder(provider: BatchInfoProvider): Builder = MessageIdBuilderImpl(provider)
    }
}

private const val SEQUENCE_NOT_SET = Long.MIN_VALUE

