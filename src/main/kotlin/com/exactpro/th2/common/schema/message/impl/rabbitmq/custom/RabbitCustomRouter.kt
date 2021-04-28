/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.message.impl.rabbitmq.custom

import com.exactpro.th2.common.schema.message.FilterFunction
import com.exactpro.th2.common.schema.message.MessageQueue
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.configuration.RouterFilter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitMessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import com.google.protobuf.Message

class RabbitCustomRouter<T : Any>(
    customTag: String,
    labels: Array<String>,
    private val converter: MessageConverter<T>,
    defaultSendAttributes: Set<String> = emptySet(),
    defaultSubscribeAttributes: Set<String> = emptySet()
) : AbstractRabbitMessageRouter<T>() {
    private val requiredSubscribeAttributes: Set<String> = hashSetOf(QueueAttribute.SUBSCRIBE.toString()) + defaultSubscribeAttributes
    private val requiredSendAttributes: Set<String> = hashSetOf(QueueAttribute.PUBLISH.toString()) + defaultSendAttributes
    private val metricsHolder = MetricsHolder(customTag, labels)

    override fun createQueue(connectionManager: ConnectionManager, queueConfiguration: QueueConfiguration, filterFunction: FilterFunction): MessageQueue<T> {
        return RabbitCustomQueue(converter, metricsHolder).apply {
            init(connectionManager, queueConfiguration, filterFunction)
        }
    }

    // FIXME: the filtering is not working for custom objects
    override fun filterMessage(msg: Message?, filters: MutableList<out RouterFilter>?): Boolean = true

    // FIXME: the filtering is not working for custom objects
    override fun findQueueByFilter(queues: Map<String, QueueConfiguration>, msg: T): Map<String, T> {
        return queues.keys.associateWithTo(HashMap()) { msg }
    }

    override fun requiredSubscribeAttributes(): Set<String> = requiredSubscribeAttributes

    override fun requiredSendAttributes(): Set<String> = requiredSendAttributes
}