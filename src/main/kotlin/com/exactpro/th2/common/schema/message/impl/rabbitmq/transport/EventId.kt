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

    companion object {
        @JvmStatic
        fun builder(): Builder = AutoBuilder_EventId_Builder()
    }
}