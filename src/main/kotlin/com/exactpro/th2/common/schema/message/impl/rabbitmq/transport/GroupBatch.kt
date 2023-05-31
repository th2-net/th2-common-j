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

data class GroupBatch(
    val book: String,
    val sessionGroup: String,
    val groups: List<MessageGroup> = emptyList(),
) {
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

    companion object {
        @JvmStatic
        fun builder(): Builder = AutoBuilder_GroupBatch_Builder()
    }
}