/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import org.jetbrains.annotations.NotNull;
import java.io.IOException;

/**
 * Interface for send and receive RabbitMQ messages
 * @param <T> messages for send and receive
 */
public interface MessageRouter<T> extends AutoCloseable {
    default void init(@NotNull MessageRouterContext context, @NotNull MessageRouter<MessageGroupBatch> groupBatchRouter) {
        init(context);
    }

    /**
     * Initialization message router
     * @param context router context
     */
    void init(@NotNull MessageRouterContext context);

    /**
     * Creates a new exclusive queue and subscribes to it. Only declaring connection can use this queue.
     * Please note Exclusive queues are deleted when their declaring connection is closed or gone (e.g. due to underlying TCP connection loss).
     * They, therefore, are only suitable for client-specific transient states.
     * @return {@link ExclusiveSubscriberMonitor} object to manage subscription.
     */
    ExclusiveSubscriberMonitor subscribeExclusive(MessageListener<T> callback);

    /**
     * Listen <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes.
     * Restrictions:
     * You can create only one subscription to th2 pin using any subscribe* functions.
     * Internal state:
     * Router uses external Connection Manage to interact with RabbitMQ, which holds one connection and one channel per th2 pin in general.
     * This rule exception is re-connect to RabbitMQ when the manager establishes new connection and creates new channels.
     * @param callback listener
     * @param queueAttr queues attributes
     * @throws IllegalStateException when more than 1 queue is found
     * @throws RuntimeException when the th2 pin is matched by passed attributes already has active subscription
     * @return {@link SubscriberMonitor} it start listening.
     */
    SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr);

    /**
     * Listen <b>SOME</b> RabbitMQ queues by intersection schemas queues attributes
     * @see #subscribe(MessageListener, String...)
     * @param callback listener
     * @param queueAttr queues attributes
     * @return {@link SubscriberMonitor} it start listening.
     */
    SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr);

    /**
     * Listen <b>ONE</b> RabbitMQ queue by intersection schemas queues attributes
     * @see #subscribe(MessageListener, String...)
     * @param queueAttr queues attributes
     * @param callback listener with manual confirmation
     * @throws IllegalStateException when more than 1 queue is found
     * @return {@link SubscriberMonitor} it start listening.
     */
    default SubscriberMonitor subscribeWithManualAck(ManualConfirmationListener<T> callback, String... queueAttr) {
        // TODO: probably should not have default implementation
        throw new UnsupportedOperationException("The subscription with manual confirmation is not supported");
    }

    /**
     * Listen <b>SOME</b> RabbitMQ queues by intersection schemas queues attributes
     * @see #subscribe(MessageListener, String...)
     * @param callback listener with manual confirmation
     * @param queueAttr queues attributes
     * @return {@link SubscriberMonitor} it start listening.
     */
    default SubscriberMonitor subscribeAllWithManualAck(ManualConfirmationListener<T> callback, String... queueAttr) {
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