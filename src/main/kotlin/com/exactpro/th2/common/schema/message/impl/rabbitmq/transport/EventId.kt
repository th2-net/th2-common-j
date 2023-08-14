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

import com.google.auto.value.AutoBuilder
import java.time.Instant

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

    @AutoBuilder
    interface Builder {
        val id: String
        val book: String
        val scope: String
        val timestamp: Instant

        fun setId(id: String): Builder
        fun setBook(book: String): Builder
        fun setScope(scope: String): Builder
        fun setTimestamp(timestamp: Instant): Builder
        fun build(): EventId
    }

    fun toBuilder(): Builder = AutoBuilder_EventId_Builder(this)

    companion object {
        @JvmStatic
        fun builder(): Builder = AutoBuilder_EventId_Builder()
    }
}