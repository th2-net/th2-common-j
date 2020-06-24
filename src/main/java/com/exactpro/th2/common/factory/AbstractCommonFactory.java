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
package com.exactpro.th2.common.factory;

import java.io.FileInputStream;
import java.io.IOException;

import com.exactpro.th2.common.loader.impl.DefaultLoader;
import com.exactpro.th2.common.message.MessageRouter;
import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.message.impl.rabbitmq.configuration.RabbitMQConfiguration;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractCommonFactory {

    private static ObjectMapper mapper = new ObjectMapper();
    private RabbitMQConfiguration rabbitMQConfiguration = null;
    private MessageRouterConfiguration messageRouterConfiguration = null;

    public MessageRouter<MessageBatch> getMessageBatchRouter() {
        MessageRouter<MessageBatch> router = DefaultLoader.INSTANCE.createInstance(MessageRouter.class, MessageBatch.class);
        router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
        return router;
    }

    public MessageRouter<RawMessageBatch> getRawMessageBatchRouter() {
        MessageRouter<RawMessageBatch> router = DefaultLoader.INSTANCE.createInstance(MessageRouter.class, RawMessageBatch.class);
        router.init(getRabbitMqConfiguration(), getMessageRouterConfiguration());
        return router;
    }

    protected abstract String getPathToRabbitMQConfiguration();
    protected abstract String getPathToMessageRouterConfiguration();

    private synchronized RabbitMQConfiguration getRabbitMqConfiguration() {
        if (rabbitMQConfiguration == null) {
            try (var in = new FileInputStream(getPathToRabbitMQConfiguration())) {
                rabbitMQConfiguration = mapper.readerFor(RabbitMQConfiguration.class).readValue(in);
            } catch (IOException e) {
                throw new IllegalStateException("Can not read rabbit mq configuration", e);
            }
        }

        return rabbitMQConfiguration;
    }

    private synchronized MessageRouterConfiguration getMessageRouterConfiguration() {
        if (messageRouterConfiguration == null) {
            try (var in = new FileInputStream(getPathToMessageRouterConfiguration())) {
                messageRouterConfiguration = mapper.readerFor(MessageRouterConfiguration.class).readValue(in);
            } catch (IOException e) {
                throw new IllegalStateException("Can not read message router configuration", e);
            }
        }

        return messageRouterConfiguration;
    }

}
