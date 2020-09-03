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

import java.io.Closeable;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.configuration.QueueConfiguration;
import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration;

/**
 * Message queue
 * @param <T>
 * @see MessageSubscriber
 * @see MessageSender
 */
public interface MessageQueue<T> extends Closeable {
    // TODO refactor to share connection instead of passing a config and creating a factory/connection for each subscriber/sender
    void init(@NotNull RabbitMQConfiguration configuration, @NotNull QueueConfiguration queueConfiguration);

    MessageSubscriber<T> getSubscriber();

    MessageSender<T> getSender();

}
