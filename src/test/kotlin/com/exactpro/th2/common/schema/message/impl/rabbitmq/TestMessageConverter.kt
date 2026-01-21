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
package com.exactpro.th2.common.schema.message.impl.rabbitmq

import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.MessageConverter

class TestMessageConverter: MessageConverter<String> {
    override fun toByteArray(value: String): ByteArray = value.toByteArray()

    override fun fromByteArray(data: ByteArray): String = String(data)

    override fun extractCount(value: String): Int = 1

    override fun toDebugString(value: String): String = value

    override fun toTraceString(value: String): String = value
}