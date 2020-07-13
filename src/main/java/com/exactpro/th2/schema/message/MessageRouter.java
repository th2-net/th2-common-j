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

public interface MessageRouter<T> {

    void init(@NotNull RabbitMQConfiguration rabbitMQConfiguration, @NotNull MessageRouterConfiguration configuration);

    //TODO: must be better architecture
    //@Nullable
    //SubscriberMonitor subscribe(Filter filter, MessageListener<T> callback);

    @Nullable
    SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback);

    @Nullable
    SubscriberMonitor subscribe(MessageListener<T> callback, String... queueAttr);

    @Nullable
    SubscriberMonitor subscribeAll(MessageListener<T> callback);

    @Nullable
    SubscriberMonitor subscribeAll(MessageListener<T> callback, String... queueAttr);

    void unsubscribeAll() throws IOException;

    void send(T message) throws IOException;

    void send(T message, String... queueAttr) throws IOException;

    void sendAll(T message, String... queueAttr) throws IOException;

}
