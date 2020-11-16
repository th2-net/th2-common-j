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

package com.exactpro.th2.common.schema.message.impl.rabbitmq;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import com.exactpro.th2.common.grpc.*;
import io.prometheus.client.Counter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);

    private final AtomicReference<String> sendQueue = new AtomicReference<>();
    private final AtomicReference<String> exchangeName = new AtomicReference<>();
    private final AtomicReference<ConnectionManager> connectionManager = new AtomicReference<>();

    private static final Counter OUTGOING_PARSED_MSG_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_parsed_batch_msg_quantity", "Quantity of outgoing parsed message batches").register();
    private static final Counter OUTGOING_PARSED_MSG_QUANTITY = Counter.build("th2_mq_outgoing_parsed_msg_quantity", "Quantity of outgoing parsed messages").register();
    private static final Counter OUTGOING_RAW_MSG_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_raw_batch_msg_quantity", "Quantity of outgoing raw message batches").register();
    private static final Counter OUTGOING_RAW_MSG_QUANTITY = Counter.build("th2_mq_outgoing_raw_msg_quantity", "Quantity of outgoing raw messages").register();
    private static final Counter OUTGOING_EVENT_BATCH_QUANTITY = Counter.build("th2_mq_outgoing_event_batch_quantity", "Quantity of outgoing event batches").register();
    private static final Counter OUTGOING_EVENT_QUANTITY = Counter.build("th2_mq_outgoing_event_quantity", "Quantity of outgoing events").register();

    @Override
    public void init(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String sendQueue) {
        Objects.requireNonNull(connectionManager, "Connection can not be null");
        Objects.requireNonNull(exchangeName, "Exchange name can not be null");
        Objects.requireNonNull(sendQueue, "Send queue can not be null");

        if (this.connectionManager.get() != null && this.sendQueue.get() != null && this.exchangeName.get() != null) {
            throw new IllegalStateException("Sender is already initialize");
        }

        this.connectionManager.set(connectionManager);
        this.exchangeName.set(exchangeName);
        this.sendQueue.set(sendQueue);
    }

    @Override
    public void send(T value) throws IOException {
        if (value instanceof MessageBatch) {
            OUTGOING_PARSED_MSG_BATCH_QUANTITY.inc();
            OUTGOING_PARSED_MSG_QUANTITY.inc(((MessageBatchOrBuilder)value).getMessagesCount());
        }

        if (value instanceof RawMessageBatch) {
            OUTGOING_RAW_MSG_BATCH_QUANTITY.inc();
            OUTGOING_RAW_MSG_QUANTITY.inc(((RawMessageBatchOrBuilder)value).getMessagesCount());
        }

        if (value instanceof EventBatch) {
            OUTGOING_EVENT_BATCH_QUANTITY.inc();
            OUTGOING_EVENT_QUANTITY.inc(((EventBatchOrBuilder)value).getEventsCount());
        }

        try {
            ConnectionManager connection = this.connectionManager.get();
            connection.basicPublish(exchangeName.get(), sendQueue.get(), null, valueToBytes(value));

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Message sent to exchangeName='{}', routing key='{}': '{}'",
                        exchangeName, sendQueue, toShortDebugString(value));
            }
        } catch (Exception e) {
            throw new IOException("Can not send message: " + toShortDebugString(value), e);
        }
    }

    protected String toShortDebugString(T value) {
        return value.toString();
    }

    protected abstract byte[] valueToBytes(T value);


}
