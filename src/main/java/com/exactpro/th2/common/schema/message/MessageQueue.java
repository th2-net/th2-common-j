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

package com.exactpro.th2.common.schema.message;

import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import org.jetbrains.annotations.NotNull;

/**
 * Message queue
 * @see MessageSubscriber
 * @see MessageSender
 */
public interface MessageQueue<T> extends AutoCloseable {
    @Deprecated(since = "3.3.0", forRemoval = true)
    void init(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration);

    void init(@NotNull ConnectionManager connectionManager, @NotNull QueueConfiguration queueConfiguration, @NotNull FilterFunction filterFunc);

    MessageSubscriber<T> getSubscriber();

    MessageSender<T> getSender();

}
