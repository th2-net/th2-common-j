/*
 * Copyright 2024-2026 Exactpro (Exactpro Systems Limited)
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

/**
 * This class holds information about book and session group from the batch root.
 * It is used to pass this information down to [MessageId].
 */
internal sealed interface BatchInfoProvider {
    val book: String
    val sessionGroup: String

    class Mutable : BatchInfoProvider {
        override lateinit var book: String
        override lateinit var sessionGroup: String
        override fun equals(other: Any?): Boolean {
            return this === other
        }

        override fun hashCode(): Int {
            return System.identityHashCode(this)
        }
    }

    object Empty : BatchInfoProvider {
        override val book: String
            get() = ""
        override val sessionGroup: String
            get() = ""
    }
}

