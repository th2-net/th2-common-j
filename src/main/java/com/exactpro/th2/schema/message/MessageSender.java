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

import javax.annotation.concurrent.NotThreadSafe;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.schema.message.impl.rabbitmq.connection.ConnectionOwner;
import com.rabbitmq.client.Connection;

/**
 * Send message to {@link MessageQueue}
 * @param <T>
 */
@NotThreadSafe
public interface MessageSender<T> extends AutoCloseable {
    void init(@NotNull ConnectionOwner connectionOwner, @NotNull String exchangeName, @NotNull String sendQueue);

    void start() throws Exception;

    boolean isOpen();

    void send(T message) throws IOException;
}
