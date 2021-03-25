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

import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.schema.message.FilterFunction
import com.exactpro.th2.common.schema.message.MessageRouterContext
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitQueue
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSubscriber
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.SubscribeTarget
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager
import io.prometheus.client.Counter
import io.prometheus.client.Histogram

class RabbitCustomQueue<T : Any>(
    messageRouterContext: MessageRouterContext,
    queueConfiguration: QueueConfiguration,
    filterFunction: FilterFunction,
    private val converter: MessageConverter<T>,
    private val metricsHolder: MetricsHolder
) : AbstractRabbitQueue<T>(messageRouterContext, queueConfiguration, filterFunction) {

    override fun createSender(
        messageRouterContext: MessageRouterContext,
        queueConfiguration: QueueConfiguration
    ): MessageSender<T> {
        return Sender(
            messageRouterContext,
            queueConfiguration.exchange,
            queueConfiguration.routingKey,
            converter,
            metricsHolder.outgoingDeliveryCounter,
            metricsHolder.outgoingDataCounter
        )
    }

    override fun createSubscriber(
        messageRouterContext: MessageRouterContext,
        queueConfiguration: QueueConfiguration,
        filterFunction: FilterFunction
    ): MessageSubscriber<T> {
        return Subscriber(
            messageRouterContext,
            SubscribeTarget(queueConfiguration.queue, queueConfiguration.routingKey, queueConfiguration.exchange),
            filterFunction,
            converter,
            metricsHolder.incomingDeliveryCounter,
            metricsHolder.processingTimer,
            metricsHolder.incomingDataCounter
        )
    }

    private class Sender<T : Any>(
        messageRouterContext: MessageRouterContext,
        exchange: String,
        routingKey: String,
        private val converter: MessageConverter<T>,
        private val deliveryCounter: Counter,
        private val dataCounter: Counter
    ) : AbstractRabbitSender<T>(messageRouterContext, exchange, routingKey) {
        override fun valueToBytes(value: T): ByteArray = converter.toByteArray(value)

        override fun toShortDebugString(value: T): String = converter.toDebugString(value)

        //region Prometheus stats
        override fun getDeliveryCounter(): Counter = deliveryCounter

        override fun getContentCounter(): Counter = dataCounter

        override fun extractCountFrom(message: T): Int = converter.extractCount(message)
        //endregion
    }

    private class Subscriber<T : Any>(
        messageRouterContext: MessageRouterContext,
        target: SubscribeTarget,
        filterFunction: FilterFunction,
        private val converter: MessageConverter<T>,
        private val deliveryCounter: Counter,
        private val timer: Histogram,
        private val dataCounter: Counter
    ) : AbstractRabbitSubscriber<T>(messageRouterContext, target, filterFunction) {
        override fun valueFromBytes(body: ByteArray): List<T> = listOf(converter.fromByteArray(body))

        override fun toShortDebugString(value: T): String = converter.toDebugString(value)

        // FIXME: the filtering is not working for custom objects
        override fun filter(value: T): T = value

        //region Prometheus stats
        override fun getDeliveryCounter(): Counter = deliveryCounter

        override fun getContentCounter(): Counter = dataCounter

        override fun getProcessingTimer(): Histogram = timer

        override fun extractCountFrom(message: T): Int = converter.extractCount(message)
        //endregion
    }
}
