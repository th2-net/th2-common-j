/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.rabbitMQ;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.message.IMessageQueueFactory;
import com.exactpro.th2.common.message.IMessageQueueSender;
import com.exactpro.th2.common.message.IMessageQueueSubscriber;
import com.exactpro.th2.rabbitMQ.configuration.IRabbitMQConfiguration;

public class RabbitMessageQueueFactory implements IMessageQueueFactory {

    private IRabbitMQConfiguration configuration = null;

    public void init(@NotNull IRabbitMQConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public IMessageQueueSubscriber createListener(String... tags) {
        RabbitMessageQueueSubscriber subscriber = new RabbitMessageQueueSubscriber();
        subscriber.init(configuration, tags);
        return subscriber;
    }

    @Override
    public IMessageQueueSender createSender(String tag) {
        RabbitMessageQueueSender sender = new RabbitMessageQueueSender();
        sender.init(configuration, tag);
        return sender;
    }
}
