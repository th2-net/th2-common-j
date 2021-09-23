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
import com.exactpro.th2.common.schema.message.MessageSender
import com.exactpro.th2.common.schema.message.MessageSubscriber
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSender
import com.exactpro.th2.common.schema.message.impl.rabbitmq.AbstractRabbitSubscriber
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.PinName
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager

/**
 * NOTE: labels are used for back compatibility and may be deleted later
 */
class RabbitCustomRouter<T : Any>(
    private val customTag: String,
    labels: Array<String>,
    private val converter: MessageConverter<T>,
    defaultSendAttributes: Set<String> = emptySet(),
    defaultSubscribeAttributes: Set<String> = emptySet()
) : AbstractRabbitRouter<T>() {
    private val requiredSendAttributes: MutableSet<String> =
        mutableSetOf(QueueAttribute.PUBLISH.toString()).apply {
            addAll(defaultSendAttributes)
        }

    private val requiredSubscribeAttributes: MutableSet<String> =
        mutableSetOf(QueueAttribute.SUBSCRIBE.toString()).apply {
            addAll(defaultSubscribeAttributes)
        }

    private val metricsHolder = MetricsHolder(customTag, labels)

    override fun getRequiredSendAttributes(): Set<String> {
        return requiredSendAttributes
    }

    override fun getRequiredSubscribeAttributes(): Set<String> {
        return requiredSubscribeAttributes
    }

    override fun splitAndFilter(message: T, pinConfiguration: PinConfiguration, pinName: PinName): T {
        return message
    }

    override fun createSender(pinConfig: PinConfiguration, pinName: PinName): MessageSender<T> {
        return Sender(
            connectionManager,
            pinConfig.exchange,
            pinConfig.routingKey,
            pinName,
            customTag,
            converter,
            metricsHolder.outgoingDeliveryCounter,
            metricsHolder.outgoingDataCounter
        )
    }

    override fun createSubscriber(pinConfig: PinConfiguration, pinName: PinName): MessageSubscriber<T> {
        return Subscriber(
            connectionManager,
            pinConfig.queue,
            FilterFunction.DEFAULT_FILTER_FUNCTION,
            pinName,
            customTag,
            converter,
            metricsHolder.incomingDeliveryCounter,
            metricsHolder.processingTimer,
            metricsHolder.incomingDataCounter
        )
    }

    override fun T.toErrorString(): String {
        return this.toString()
    }

    override fun getDeliveryCounter(): Counter {
        return metricsHolder.outgoingDeliveryCounter
    }

    override fun getContentCounter(): Counter {
        return metricsHolder.outgoingDataCounter
    }

    override fun extractCountFrom(batch: T): Int {
        return converter.extractCount(batch)
    }

    private class Sender<T : Any>(
        connectionManager: ConnectionManager,
        exchangeName: String,
        routingKey: String,
        th2Pin: String,
        customTag: String,
        private val converter: MessageConverter<T>,
        private val deliveryCounter: Counter,
        private val dataCounter: Counter
    ) : AbstractRabbitSender<T>(connectionManager, exchangeName, routingKey, th2Pin, customTag) {
        override fun valueToBytes(value: T): ByteArray = converter.toByteArray(value)

        override fun toShortTraceString(value: T): String = converter.toTraceString(value)

        override fun toShortDebugString(value: T): String = converter.toDebugString(value)
    }

    private class Subscriber<T : Any>(
        connectionManager: ConnectionManager,
        queue: String,
        filterFunction: FilterFunction,
        th2Pin: String,
        customTag: String,
        private val converter: MessageConverter<T>,
        private val deliveryCounter: Counter,
        private val timer: Histogram,
        private val dataCounter: Counter
    ) : AbstractRabbitSubscriber<T>(connectionManager, queue, filterFunction, th2Pin, customTag) {
        override fun valueFromBytes(body: ByteArray): T = converter.fromByteArray(body)

        override fun toShortTraceString(value: T): String = converter.toTraceString(value)

        override fun toShortDebugString(value: T): String = converter.toDebugString(value)

        // FIXME: the filtering is not working for custom objects
        override fun filter(value: T): T = value

        //region Prometheus stats
        override fun getDeliveryCounter(): Counter = deliveryCounter

        override fun getContentCounter(): Counter = dataCounter

        override fun getProcessingTimer(): Histogram = timer

        override fun extractCountFrom(batch: T): Int = converter.extractCount(batch)

        override fun extractLabels(batch: T): Array<String> = converter.getLabels(batch)

        //endregion
    }
}