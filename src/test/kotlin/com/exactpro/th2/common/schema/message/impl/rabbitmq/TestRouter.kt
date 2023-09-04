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
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber

internal class TestRouter : AbstractRabbitRouter<String>() {
    override fun splitAndFilter(message: String, pinConfiguration: PinConfiguration, pinName: PinName): String {
        return message
    }

    override fun createSender(
        pinConfig: PinConfiguration,
        pinName: PinName,
        bookName: BookName
    ): MessageSender<String> = TestSender(
        connectionManager,
        pinConfig.exchange,
        pinConfig.routingKey,
        pinName,
        bookName
    )

    override fun String.toErrorString(): String = "Error $this"

    override fun createSubscriber(
        pinConfig: PinConfiguration,
        pinName: PinName,
        listener: ConfirmationListener<String>
    ): MessageSubscriber = TestSubscriber(
        connectionManager,
        pinConfig.queue,
        pinName,
        listener
    )

}