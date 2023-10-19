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

enum class Direction(val id: Int) {
    /**
     * Related to messages are income to a client
     */
    INCOMING(1),
    /**
     * Related to messages are out gone from a client
     */
    OUTGOING(2);

    companion object {
        fun forId(id: Int): Direction = when (id) {
            1 -> INCOMING
            2 -> OUTGOING
            else -> error("Unknown direction id: $id")
        }
    }
}