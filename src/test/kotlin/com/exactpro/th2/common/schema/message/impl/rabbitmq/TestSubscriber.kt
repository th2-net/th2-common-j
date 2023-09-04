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
package com.exactpro.th2.common.schema.message.impl.rabbitmq

import com.exactpro.th2.common.schema.message.ConfirmationListener
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager

internal class TestSubscriber(
    connectionManager: ConnectionManager,
    queue: String,
    th2Pin: String,
    listener: ConfirmationListener<String>
) : AbstractRabbitSubscriber<String>(
    connectionManager,
    queue,
    th2Pin,
    "test-string",
    listener
) {
    override fun valueFromBytes(body: ByteArray): String = String(body)

    override fun filter(value: String): String = value

    override fun toShortDebugString(value: String): String = value

    override fun toShortTraceString(value: String): String = value
}