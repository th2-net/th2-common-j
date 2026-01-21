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

import java.time.Instant
import java.util.StringJoiner

data class EventId(
    val id: String,
    val book: String,
    val scope: String,
    val timestamp: Instant,
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as EventId

        if (id != other.id) return false
        if (book != other.book) return false
        if (scope != other.scope) return false
        return timestamp == other.timestamp
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + book.hashCode()
        result = 31 * result + scope.hashCode()
        result = 31 * result + timestamp.hashCode()
        return result
    }

    override fun toString(): String {
        return "EventId(id='$id', book='$book', scope='$scope', timestamp=$timestamp)"
    }

    interface Builder {
        val id: String
        fun isIdSet(): Boolean
        val book: String
        fun isBookSet(): Boolean
        val scope: String
        fun isScopeSet(): Boolean
        val timestamp: Instant
        fun isTimestampSet(): Boolean

        fun setId(id: String): Builder
        fun setBook(book: String): Builder
        fun setScope(scope: String): Builder
        fun setTimestamp(timestamp: Instant): Builder
        fun build(): EventId
    }

    fun toBuilder(): Builder = BuilderImpl(this)

    companion object {
        @JvmStatic
        fun builder(): Builder = BuilderImpl()
    }

}

private class BuilderImpl : EventId.Builder {
    private var _id: String? = null
    private var _book: String? = null
    private var _scope: String? = null
    private var _timestamp: Instant? = null

    constructor()
    constructor(source: EventId) {
        _id = source.id
        _book = source.book
        _scope = source.scope
        _timestamp = source.timestamp
    }

    override fun setId(id: String): EventId.Builder = apply {
        this._id = id
    }

    override val id: String
        get() = checkNotNull(_id) { "Property \"id\" has not been set" }

    override fun isIdSet(): Boolean = _id != null

    override fun setBook(book: String): EventId.Builder = apply {
        this._book = book
    }

    override val book: String
        get() = checkNotNull(_book) { "Property \"book\" has not been set" }

    override fun isBookSet(): Boolean = _book != null

    override fun setScope(scope: String): EventId.Builder = apply {
        this._scope = scope
    }

    override val scope: String
        get() = checkNotNull(_scope) { "Property \"scope\" has not been set" }

    override fun isScopeSet(): Boolean = _scope != null

    override fun setTimestamp(timestamp: Instant): EventId.Builder = apply {
        this._timestamp = timestamp
    }

    override val timestamp: Instant
        get() {
            return checkNotNull(_timestamp) { "Property \"timestamp\" has not been set" }
        }

    override fun isTimestampSet(): Boolean = _timestamp != null

    override fun build(): EventId {
        if (_id == null || _book == null || _scope == null || _timestamp == null) {
            val missing = StringJoiner(",", "[", "]")
            if (_id == null) {
                missing.add("id")
            }
            if (_book == null) {
                missing.add("book")
            }
            if (_scope == null) {
                missing.add("scope")
            }
            if (_timestamp == null) {
                missing.add("timestamp")
            }
            error("Missing required properties: $missing")
        }
        return EventId(
            _id!!,
            _book!!,
            _scope!!,
            _timestamp!!,
        )
    }
}
