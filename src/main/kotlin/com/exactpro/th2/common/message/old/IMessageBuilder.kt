/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.message.old

import java.time.Instant

interface IMessageBuilder<Message> {
    fun sessionAlias(sessionAlias: String): IMessageBuilder<Message>

    fun direction(direction: Int): IMessageBuilder<Message>

    fun sequence(sequence: Long): IMessageBuilder<Message>

    fun addSubsequence(subsequence: Int): IMessageBuilder<Message>

    fun timestamp(timestamp: Instant): IMessageBuilder<Message>

    fun addProperty(key: String, value: String): IMessageBuilder<Message>

    fun protocol(protocol: String): IMessageBuilder<Message>

    fun build(parentEventId: String?): Message
}