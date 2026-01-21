/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.schema.message

import com.exactpro.th2.common.schema.exception.RouterException

/**
 * Interface for send and receive RabbitMQ notification messages
 *
 * @param <T> messages for send and receive
 */
interface NotificationRouter<T> : AutoCloseable {
    /**
     * Initialization message router
     * @param context router context
     */
    fun init(context: MessageRouterContext)

    /**
     * Send message to exclusive RabbitMQ queue
     *
     * @param message
     * @throws com.exactpro.th2.common.schema.exception.RouterException if it cannot send message
     */
    @Throws(RouterException::class)
    fun send(message: T)

    /**
     * Listen exclusive RabbitMQ queue
     *
     * @param callback listener
     * @return SubscriberMonitor if starts listening
     */
    fun subscribe(callback: MessageListener<T>): SubscriberMonitor
}