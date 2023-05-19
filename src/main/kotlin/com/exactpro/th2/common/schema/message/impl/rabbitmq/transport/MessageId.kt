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
    @AutoBuilder
    interface Builder {
        fun setSessionAlias(sessionAlias: String): Builder
        fun setDirection(direction: Direction): Builder
        fun setSequence(sequence: Long): Builder
        fun setTimestamp(timestamp: Instant): Builder
        fun subsequenceBuilder(): CollectionBuilder<Int>
        fun addSubsequence(subsequnce: Int): Builder = apply {
            subsequenceBuilder().add(subsequnce)
        }
        fun setSubsequence(subsequence: List<Int>): Builder
        fun build(): MessageId
    }

    fun toBuilder(): MessageId.Builder = AutoBuilder_MessageId_Builder(this)

    companion object {
        @JvmStatic
        fun builder(): Builder = AutoBuilder_MessageId_Builder()
    }
}