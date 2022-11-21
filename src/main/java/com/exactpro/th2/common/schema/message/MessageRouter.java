/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message;

import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.schema.message.impl.context.DefaultMessageRouterContext;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Objects;

/**
 * Interface for send and receive RabbitMQ messages
 * @param <T> messages for send and receive
 */
public interface MessageRouter<T> extends AutoCloseable {

    /**
     * Initialization message router
     * @param configuration message router configuration
     */
    @Deprecated(since = "3.2.2", forRemoval = true)
    default void init(@NotNull ConnectionManager connectionManager, @NotNull MessageRouterConfiguration configuration) {
        Objects.requireNonNull(connectionManager, "Connection owner can not be null");
        Objects.requireNonNull(configuration, "Configuration cannot be null");

        init(new DefaultMessageRouterContext(connectionManager, MessageRouterMonitor.DEFAULT_MONITOR, configuration));
    }

    default void init(@NotNull MessageRouterContext context, @NotNull MessageRouter<MessageGroupBatch> groupBatchRouter) {
        init(context);
    }

    /**
     * Initialization message router
     * @param context router context
     */
    void init(@NotNull MessageRouterContext context);

    /**
     * Create new exclusive queue and subscribe to it. Only declaring connection can use this queue.
     * Please note Exclusive queues are deleted when their declaring connection is closed or gone (e.g. due to underlying TCP connection loss).
     * They therefore are only suitable for client-specific transient state.
     * @return {@link SubscriberExclusiveMonitor} object to manage subscription.
     */
    SubscriberExclusiveMonitor subscribeExclusive(MessageListener<T> callback);

    /**
     * Listen <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes
     * @param callback listener
     * @param queueAttr queues attributes
     * @throws IllegalStateException when more than 1 queue is found
     * @return {@link SubscriberMonitor} it start listening. Returns null if can not listen to this queue
     */
    SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr);

    /**
     * Listen <b>ALL</b> RabbitMQ queues in configurations
     * @param callback listener
     * @return {@link SubscriberMonitor} it start listening. Returns null if can not listen to this queue
     */
    default SubscriberMonitor subscribeAll(MessageListener<T> callback) {
        return subscribeAll(callback, QueueAttribute.SUBSCRIBE.toString());
    }

    /**
     * Listen <b>SOME</b> RabbitMQ queues by intersection schemas queues attributes
     * @param callback listener
     * @param queueAttr queues attributes
     * @return {@link SubscriberMonitor} it start listening. Returns null if can not listen to this queue
     */
    SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr);

    /**
     * Listen <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes
     * @param queueAttr queues attributes
     * @param callback listener with manual confirmation
     * @throws IllegalStateException when more than 1 queue is found
     * @return {@link SubscriberMonitor} it start listening. Returns null if can not listen to this queue
     */
    default SubscriberMonitor subscribeWithManualAck(ConfirmationMessageListener<T> callback, String... queueAttr) {
        // TODO: probably should not have default implementation
        throw new UnsupportedOperationException("The subscription with manual confirmation is not supported");
    }

    /**
     * Listen <b>ALL</b> RabbitMQ queues in configurations
     * @param callback listener with manual confirmation
     * @return {@link SubscriberMonitor} it start listening. Returns null if can not listen to this queue
     */
    default SubscriberMonitor subscribeAllWithManualAck(ConfirmationMessageListener<T> callback) {
        // TODO: probably should not have default implementation
        return subscribeAllWithManualAck(callback, QueueAttribute.SUBSCRIBE.toString());
    }

    /**
     * Listen <b>SOME</b> RabbitMQ queues by intersection schemas queues attributes
     * @param callback listener with manual confirmation
     * @param queueAttr queues attributes
     * @return {@link SubscriberMonitor} it start listening. Returns null if can not listen to this queue
     */
    default SubscriberMonitor subscribeAllWithManualAck(ConfirmationMessageListener<T> callback, String... queueAttr) {
        // TODO: probably should not have default implementation
        throw new UnsupportedOperationException("The subscription with manual confirmation is not supported");
    }

    /**
     * Send the message to the queue
     * @throws IOException if router can not send message
     */
    void sendExclusive(String queue, T message) throws IOException;

    /**
     * Send message to <b>SOME</b> RabbitMQ queues which match the filter for this message
     * @throws IOException if router can not send message
     */
    default void send(T message) throws IOException {
        send(message, QueueAttribute.PUBLISH.toString());
    }

    /**
     * Send message to <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes
     * @param queueAttr schemas queues attributes
     * @throws IOException if can not send message
     * @throws IllegalStateException when more than 1 queue is found
     */
    void send(T message, String... queueAttr) throws IOException;

    /**
     * Send message to <b>SOME</b> RabbitMQ queue by intersection schemas queues attributes
     * @param queueAttr schemas queues attributes
     * @throws IOException if can not send message
     */
    void sendAll(T message, String... queueAttr) throws IOException;

}
