/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package com.exactpro.th2.schema.message;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.exactpro.th2.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;

/**
 * Interface for send and receive RabbitMQ messages
 * @param <T> messages for send and receive
 */
public interface MessageRouter<T> extends AutoCloseable {

    /**
     * Initialization message router
     * @param rabbitMQConfiguration
     * @param configuration message router configuration
     */
    void init(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull MessageRouterConfiguration configuration);

    /**
     * Listen RabbitMQ queue by schemas queues alias
     * @param queueAlias queues alias
     * @param callback listener
     * @return {@link SubscriberMonitor} it start listening. Returns null is can not listen this queue
     *
     * @deprecated please use {@link MessageRouter#subscribe(MessageListener, String...)}
     */
    @Nullable
    SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback);

    /**
     * Listen <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes
     * @param queueAttr queues attributes
     * @param callback listener
     * @throws IllegalStateException when more than 1 queue is found
     * @return {@link SubscriberMonitor} it start listening. Returns null is can not listen this queue
     */
    @Nullable
    SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr);

    /**
     * Listen <b>ALL</b> RabbitMQ queues in configurations
     * @param callback listener
     * @return {@link SubscriberMonitor} it start listening. Returns null is can not listen this queue
     */
    @Nullable
    SubscriberMonitor subscribeAll(MessageListener<T> callback);

    /**
     * Listen <b>SOME</b> RabbitMQ queues by intersection schemas queues attributes
     * @param callback listener
     * @param queueAttr queues attributes
     * @return {@link SubscriberMonitor} it start listening. Returns null is can not listen this queue
     */
    @Nullable
    SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr);

    /**
     * Unsubscribe from all queues
     * @throws IOException if can not unsubscribe from one or more queues. See suppressed exception
     */
    @Deprecated(forRemoval = true)
    void unsubscribeAll() throws IOException;

    /**
     * Send message to <b>SOME</b> RabbitMQ queues which match the filter for this message
     * @param message
     * @throws IOException if can not send message
     */
    void send(T message) throws IOException;

    /**
     * Send message to <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes
     * @param message
     * @param queueAttr schemas queues attributes
     * @throws IOException if can not send message
     * @throws IllegalStateException when more than 1 queue is found
     */
    void send(T message, String... queueAttr) throws IOException;

    /**
     * Send message to <b>SOME</b> RabbitMQ queue by intersection schemas queues attributes
     * @param message
     * @param queueAttr schemas queues attributes
     * @throws IOException if can not send message
     */
    void sendAll(T message, String... queueAttr) throws IOException;

}
