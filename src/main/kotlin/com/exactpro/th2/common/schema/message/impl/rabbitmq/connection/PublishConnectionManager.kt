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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection

import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.rabbitmq.client.AMQP
import com.rabbitmq.client.BlockedListener
import com.rabbitmq.client.Channel
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.util.concurrent.ExecutorService
import java.util.concurrent.ScheduledExecutorService

class PublishConnectionManager(
    connectionName: String,
    rabbitMQConfiguration: RabbitMQConfiguration,
    connectionManagerConfiguration: ConnectionManagerConfiguration,
    sharedExecutor: ExecutorService,
    channelChecker: ScheduledExecutorService
) : ConnectionManager(connectionName, rabbitMQConfiguration, connectionManagerConfiguration, sharedExecutor, channelChecker) {
    @Volatile var isPublishingBlocked = false

    @Throws(IOException::class, InterruptedException::class)
    fun basicPublish(exchange: String, routingKey: String, props: AMQP.BasicProperties?, body: ByteArray) {
        val holder = getOrCreateChannelFor(PinId.forRoutingKey(exchange, routingKey))
        holder.retryingPublishWithLock(configuration, body) { channel: Channel, payload: ByteArray ->
            channel.basicPublish(exchange, routingKey, props, payload)
        }
    }

    override fun createBlockedListener(): BlockedListener = object : BlockedListener {
        override fun handleBlocked(reason: String) {
            isPublishingBlocked = true
            LOGGER.info { "RabbitMQ blocked publish connection: $reason" }
        }

        override fun handleUnblocked() {
            isPublishingBlocked = false
            LOGGER.info("RabbitMQ unblocked publish connection")
        }
    }

    override fun recoverSubscriptionsOfChannel(pinId: PinId, channel: Channel, holder: ChannelHolder) {}

    companion object {
        private val LOGGER = KotlinLogging.logger {}
    }
}