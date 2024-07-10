/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
 *
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection

import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation
import com.exactpro.th2.common.schema.message.impl.OnlyOnceConfirmation.Companion.wrap
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.BlockedListener
import com.rabbitmq.client.ShutdownSignalException

import mu.KotlinLogging
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

class ConsumeConnectionManager(
    connectionName: String,
    rabbitMQConfiguration: RabbitMQConfiguration,
    connectionManagerConfiguration: ConnectionManagerConfiguration
) : ConnectionManager(connectionName, rabbitMQConfiguration, connectionManagerConfiguration) {
    private val subscriberName = if (connectionManagerConfiguration.subscriberName.isNullOrBlank()) {
        (DEFAULT_SUBSCRIBER_NAME_PREFIX + System.currentTimeMillis()).also {
            LOGGER.info { "Subscribers will use the default name: $it" }
        }
    } else {
        connectionManagerConfiguration.subscriberName + "." + System.currentTimeMillis()
    }
    private val nextSubscriberId: AtomicInteger = AtomicInteger(1)

    override fun createBlockedListener(): BlockedListener = object : BlockedListener {
        override fun handleBlocked(reason: String) {
            LOGGER.info { "RabbitMQ blocked consume connection: $reason" }
        }

        override fun handleUnblocked() {
            LOGGER.info("RabbitMQ unblocked consume connection")
        }
    }

    @Throws(IOException::class)
    fun queueDeclare(): String {
        val holder = ChannelHolder(this::createChannel, this::waitForConnectionRecovery, configurationToOptions())
        return holder.mapWithLock { channel ->
            channel.queueDeclare(
                "",  // queue name
                false,  // durable
                true,  // exclusive
                false,  // autoDelete
                emptyMap()
            ).queue.also { queue ->
                LOGGER.info { "Declared exclusive '$queue' queue" }
                putChannelFor(PinId.forQueue(queue), holder)
            }
        }
    }

    @Throws(IOException::class, TimeoutException::class)
    fun queueExclusiveDeclareAndBind(exchange: String): String {
        createChannel().use { channel ->
            val queue = channel.queueDeclare().queue
            channel.queueBind(queue, exchange, EMPTY_ROUTING_KEY)
            LOGGER.info { "Declared the '$queue' queue to listen to the '$exchange'" }
            return queue
        }
    }

    @Throws(IOException::class, InterruptedException::class)
    fun basicConsume(
        queue: String,
        deliverCallback: ManualAckDeliveryCallback,
        cancelCallback: CancelCallback
    ): ExclusiveSubscriberMonitor {
        val pinId = PinId.forQueue(queue)
        val holder = getOrCreateChannelFor(pinId, SubscriptionCallbacks(deliverCallback, cancelCallback))
        val tag = holder.retryingConsumeWithLock({ channel: Channel ->
            channel.basicConsume(
                queue, false, subscriberName + "_" + nextSubscriberId.getAndIncrement(),
                { tagTmp: String, delivery: Delivery ->
                    try {
                        val envelope = delivery.envelope
                        val deliveryTag = envelope.deliveryTag
                        val routingKey = envelope.routingKey
                        LOGGER.trace { "Received delivery $deliveryTag from queue=$queue routing_key=$routingKey" }

                        val wrappedConfirmation: Confirmation = object : Confirmation {
                            @Throws(IOException::class)
                            override fun reject() {
                                holder.withLock { ch: Channel ->
                                    try {
                                        ch.basicReject(deliveryTag, false)
                                    } catch (e: IOException) {
                                        LOGGER.warn { "Error during basicReject of message with deliveryTag = $deliveryTag inside channel #${ch.channelNumber}: $e" }
                                        throw e
                                    } catch (e: ShutdownSignalException) {
                                        LOGGER.warn { "Error during basicReject of message with deliveryTag = $deliveryTag inside channel #${ch.channelNumber}: $e" }
                                        throw e
                                    } finally {
                                        holder.release { metrics.readinessMonitor.enable() }
                                    }
                                }
                            }

                            @Throws(IOException::class)
                            override fun confirm() {
                                holder.withLock { ch: Channel ->
                                    try {
                                        // because delivery tags are scoped per channel,
                                        // deliveries must be acknowledged on the same channel they were received on.
                                        ch.basicAck(deliveryTag, false)
                                    } catch (e: Exception) {
                                        LOGGER.warn { "Error during basicAck of message with deliveryTag = $deliveryTag inside channel #${ch.channelNumber}: $e" }
                                        throw e
                                    } finally {
                                        holder.release { metrics.readinessMonitor.enable() }
                                    }
                                }
                            }
                        }

                        val confirmation = wrap("from $routingKey to $queue", wrappedConfirmation)

                        holder.withLock(Runnable {
                            holder.acquireAndSubmitCheck {
                                channelChecker.schedule<Boolean>(
                                    {
                                        holder.withLock(Runnable {
                                            LOGGER.warn { "The confirmation for delivery $deliveryTag in queue=$queue routing_key=$routingKey was not invoked within the specified delay" }
                                            if (holder.reachedPendingLimit()) {
                                                metrics.readinessMonitor.disable()
                                            }
                                        })
                                        false // to cast to Callable
                                    },
                                    configuration.confirmationTimeout.toMillis(),
                                    TimeUnit.MILLISECONDS
                                )
                            }
                        })
                        val redeliver = envelope.isRedeliver
                        deliverCallback.handle(DeliveryMetadata(tagTmp, redeliver), delivery, confirmation)
                    } catch (e: Exception) {
                        LOGGER.error("Cannot handle delivery for tag $tagTmp: ${e.message}", e)
                    } catch (e: RuntimeException) {
                        LOGGER.error("Cannot handle delivery for tag $tagTmp: ${e.message}", e)
                    }
                }, cancelCallback
            )
        }, configuration)

        return RabbitMqSubscriberMonitor(holder, queue, tag) { channel, consumerTag -> channel.basicCancel(consumerTag) }
    }

    override fun recoverSubscriptionsOfChannel(pinId: PinId, channel: Channel, holder: ChannelHolder) {
        channelChecker.execute {
            holder.withLock(Runnable {
                try {
                    val subscriptionCallbacks = holder.getCallbacksForRecovery(channel)

                    if (subscriptionCallbacks != null) {
                        LOGGER.info { "Changing channel for holder with pin id: $pinId" }

                        val removedHolder = channelsByPin.remove(pinId)
                        check(removedHolder === holder) { "Channel holder has been replaced" }

                        basicConsume(
                            pinId.queue,
                            subscriptionCallbacks.deliverCallback,
                            subscriptionCallbacks.cancelCallback
                        )
                    }
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    LOGGER.info("Recovering channel's subscriptions interrupted", e)
                } catch (e: Throwable) {
                    // this code executed in executor service and exception thrown here will not be handled anywhere
                    LOGGER.error("Failed to recovery channel's subscriptions", e)
                }
            })
        }
    }

    private class RabbitMqSubscriberMonitor(
        private val holder: ChannelHolder,
        override val queue: String,
        private val tag: String,
        private val action: CancelAction
    ) : ExclusiveSubscriberMonitor {
        @Throws(IOException::class)
        override fun unsubscribe() {
            holder.unsubscribeWithLock(tag, action)
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val DEFAULT_SUBSCRIBER_NAME_PREFIX = "rabbit_mq_subscriber."
    }
}