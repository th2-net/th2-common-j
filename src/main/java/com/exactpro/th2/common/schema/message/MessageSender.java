/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

@NotThreadSafe
public interface MessageSender<T> {

    // Please use constructor for initialization
    @Deprecated
    void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String routingKey);

    void send(T message) throws IOException;
}
