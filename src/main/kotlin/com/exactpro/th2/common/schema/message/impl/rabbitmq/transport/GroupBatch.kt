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
import com.google.auto.value.AutoBuilder

data class GroupBatch(
    val book: String,
    val sessionGroup: String,
    val groups: List<MessageGroup> = emptyList(),
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as GroupBatch

        if (book != other.book) return false
        if (sessionGroup != other.sessionGroup) return false
        return groups == other.groups
    }

    override fun hashCode(): Int {
        var result = book.hashCode()
        result = 31 * result + sessionGroup.hashCode()
        result = 31 * result + groups.hashCode()
        return result
    }

    override fun toString(): String {
        return "GroupBatch(book='$book', sessionGroup='$sessionGroup', groups=$groups)"
    }

    @AutoBuilder
    interface Builder {
        val book: String
        val sessionGroup: String

        fun setBook(book: String): Builder
        fun setSessionGroup(sessionGroup: String): Builder
        fun groupsBuilder(): CollectionBuilder<MessageGroup>
        fun addGroup(group: MessageGroup): Builder = apply {
            groupsBuilder().add(group)
        }

        fun setGroups(groups: List<MessageGroup>): Builder
        fun build(): GroupBatch
    }

    fun toBuilder(): Builder = AutoBuilder_GroupBatch_Builder(this)

    companion object {
        @JvmStatic
        fun builder(): Builder = AutoBuilder_GroupBatch_Builder()
    }
}