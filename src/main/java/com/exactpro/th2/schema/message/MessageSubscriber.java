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

import javax.annotation.concurrent.NotThreadSafe;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.impl.rabbitmq.configuration.SubscribeTarget;
import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;
import com.rabbitmq.client.Connection;

/**
 * Listen messages and transmit it to {@link MessageListener}
 *
 * @param <T>
 */
@NotThreadSafe
public interface MessageSubscriber<T> extends AutoCloseable {
    void init(@NotNull ConnectionOwner connectionOwner, @NotNull String exchangeName, @NotNull String subscriberName, @NotNull SubscribeTarget... subscribeTargets);

    void start() throws Exception;

    boolean isOpen();

    void addListener(MessageListener<T> messageListener);
}
