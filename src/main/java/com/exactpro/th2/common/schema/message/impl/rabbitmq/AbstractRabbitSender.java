/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.metrics.HealthMetrics;
import com.exactpro.th2.common.schema.message.MessageSender;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.connection.ConnectionManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ShutdownSignalException;
import io.prometheus.client.Counter;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public abstract class AbstractRabbitSender<T> implements MessageSender<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRabbitSender.class);
    private static final AtomicLong SEND_DATA_REQUEST_COUNTER = new AtomicLong(0);

    private final ConnectionManager connectionManager;
    private final ResendMessageConfiguration resendMessageConfiguration;
    private final String routingKey;
    private final String exchangeName;
    private final HealthMetrics healthMetrics = new HealthMetrics(this); //TODO: Pass attempts count

    /**
     * Scheduled executor service with single thread. It provide ability to write task oriented code without concurrent affects
     */
    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);;
    private final ManualResetEvent manualResetEvent = new ManualResetEvent(true);
    private final Set<SendData> dataToSend = ConcurrentHashMap.newKeySet();

    //region Variables below and them state can be changed in scheduler thread only in single thread mode
    private final Map<Integer, Map<Long, SendRequest>> activeSends = new HashMap<>();
    private final Set<SendData> rejectedSends = new HashSet<>();
    private Channel channel;
    // endregion

    protected AbstractRabbitSender(@NotNull ConnectionManager connectionManager, @NotNull String exchangeName, @NotNull String routingKey) {
        this.connectionManager = requireNonNull(connectionManager, "Connection manager can not be null");
        this.exchangeName = requireNonNull(exchangeName, "Exchange name can not be null");
        this.routingKey = requireNonNull(routingKey, "Send queue can not be null");
        this.resendMessageConfiguration = connectionManager.getConfiguration().getResendMessageConfiguration();

        LOGGER.info("{}:{} initialised with queue {}", getClass().getSimpleName(), hashCode(), routingKey);
    }

    @Override
    public void send(T value) {
        requireNonNull(value, "Value for send can not be null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Try to send message to exchangeName='{}', routing key='{}': '{}'",
                    exchangeName, routingKey, toShortDebugString(value));
        }
        SendData sendData = null;
        try {
            waitOne();
            sendData = new SendData(valueToBytes(value), extractCountFrom(value));
            if (!dataToSend.add(sendData)) {
                throw new IllegalStateException("Send data is already in process " + toShortDebugString(value));
            }
            publish(sendData);
            sendData.completableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Can not send message to exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, toShortDebugString(value), e);
            }
        } finally {
            if (sendData != null && !dataToSend.remove(sendData)) {
                LOGGER.warn("Send request already removed exchangeName='{}', routing key ='{}'", exchangeName, routingKey);
            }
        }
    }

    /**
     * Calls passed action and catches exception from it to manage channel.
     * This method should be called from {@link #scheduler} thread only in single thread mode
     */
    private void callOnChannel(ChannelAction channelAction) throws Exception {
        try {
            if (channel == null) {
                LOGGER.info("Try to create new channel");
                channel = connectionManager.createChannelWithoutCheck();
                channel.confirmSelect();
                channel.addConfirmListener(new SenderConfirmListener(channel.getChannelNumber(), this::acknowledge, this::notAcknowledge));
                LOGGER.info("Created new channel {}", channel.getChannelNumber());
            }

            channelAction.apply(channel);
            healthMetrics.enable();
        } catch (IOException | ShutdownSignalException e) {
            healthMetrics.notReady();
            if (channel != null) {
                try {
                    activeSends.remove(channel.getChannelNumber());
                    channel.abort();
                } catch (IOException ex) {
                    LOGGER.warn("Can not abort channel from retry request for RabbitMQ", ex);
                    ex.addSuppressed(e);
                    throw ex;
                } finally {
                    channel = null;
                }
            }
            throw e;
        }

    }

    private void publish(SendData sendData, int attempt) {
        if (LOGGER.isTraceEnabled() && attempt > 1) {
            LOGGER.trace("Try to resend message to exchangeName='{}', routing key ='{}', attempt = {}, data = {}", exchangeName, routingKey, attempt, sendData);
        }

        scheduler.schedule(() -> {
            try {
                callOnChannel(channel -> {
                    long sequence = channel.getNextPublishSeqNo();
                    SendRequest sendRequest = new SendRequest(sendData, attempt);
                    Map<Long, SendRequest> sequenceToSendRequest = activeSends.computeIfAbsent(channel.getChannelNumber(), k -> new HashMap<>());
                    checkPrevious(null, sequenceToSendRequest.put(sequence, sendRequest));
                    try {
                        channel.basicPublish(exchangeName, routingKey, null, sendRequest.sendData.data);
                    } catch (IOException | ShutdownSignalException e) { // soft exceptions
                        LOGGER.warn("Can not execute basic publish exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, sendData, e);
                        checkPrevious(sendRequest, sequenceToSendRequest.remove(sequence));
                        throw e;
                    } catch (Exception e) {
                        LOGGER.error("Exception during basic publish exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, sendData, e);
                        checkPrevious(sendRequest, sequenceToSendRequest.remove(sequence));
                        sendData.completableFuture.completeExceptionally(e);
                        throw e;
                    }
                });
            } catch (IOException | ShutdownSignalException e) {
                LOGGER.error("Can not execute publish exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, sendData, e);
                publish(sendData, attempt + 1);
            } catch (Exception e) {
                LOGGER.error("Problem with publish exchangeName='{}', routing key ='{}': '{}'", exchangeName, routingKey, sendData, e);
            }
        }, getNextDelay(attempt), TimeUnit.MILLISECONDS);
    }

    private void publish(SendData sendData) {
        publish(sendData, 1);
    }

    private void acknowledge(int channelNumber, long deliveryTag, boolean multiple) {
        scheduler.submit(() -> {
            try {
                Map<Long, SendRequest> sequenceToSendRequest = activeSends.get(channel.getChannelNumber());
                if (sequenceToSendRequest == null) {
                    LOGGER.warn("Postmortem acknowledgement exchangeName='{}', routing key ='{}', channel = {} deliveryTag = {}, multiple = {}",
                            exchangeName, routingKey, channelNumber, deliveryTag, multiple);
                    return;
                }

                if (multiple) {
                    Set<Long> sequenceToAck = sequenceToSendRequest.keySet().stream()
                            .filter(sequence -> sequence <= deliveryTag)
                            .collect(Collectors.toSet());
                    for (Long sequence : sequenceToAck) {
                        acknowledge(sequenceToSendRequest, channelNumber, sequence);
                    }
                } else {
                    acknowledge(sequenceToSendRequest, channelNumber, deliveryTag);
                }
            } catch (Exception e) {
                LOGGER.error("Problem with acknowledge exchangeName='{}', routing key ='{}', channel = {} deliveryTag = {}, multiple = {}",
                        exchangeName, routingKey, channelNumber, deliveryTag, multiple, e);
            }
        });
    }

    private void acknowledge(Map<Long, SendRequest> sequenceToSendRequest, int channelNumber, long deliveryTag) {
        SendRequest removedRequest = sequenceToSendRequest.remove(deliveryTag);
        if (removedRequest == null) {
            LOGGER.error("Unexpected acknowledgment exchangeName='{}', routing key ='{}', channel = {} deliveryTag = {}", exchangeName, routingKey, channelNumber, deliveryTag);
            return;
        }
        
        getDeliveryCounter().inc();
        getContentCounter().inc(removedRequest.sendData.size);

        try {
            if (rejectedSends.remove(removedRequest.sendData)
                    && rejectedSends.isEmpty()) {
                LOGGER.info("Send data removed from rejected list exchangeName='{}', routing key ='{}', data = {}", exchangeName, routingKey, removedRequest.sendData);
                manualResetEvent.set();
            }
        } finally {
            removedRequest.sendData.completableFuture.complete(null);
        }
    }

    private void notAcknowledge(int channelNumber, long deliveryTag, boolean multiple) {
        scheduler.submit(() -> {
            try {
                Map<Long, SendRequest> sequenceToSendRequest = activeSends.get(channel.getChannelNumber());
                if (sequenceToSendRequest == null) {
                    LOGGER.warn("Postmortem acknowledgement exchangeName='{}', routing key ='{}', channel = {} deliveryTag = {}, multiple = {}",
                            exchangeName, routingKey, channelNumber, deliveryTag, multiple);
                    return;
                }

                if (multiple) {
                    Set<Long> sequenceToAck = sequenceToSendRequest.keySet().stream()
                            .filter(sequence -> sequence <= deliveryTag)
                            .collect(Collectors.toSet());
                    for (Long sequence : sequenceToAck) {
                        notAcknowledge(sequenceToSendRequest, channelNumber, sequence);
                    }
                } else {
                    notAcknowledge(sequenceToSendRequest, channelNumber, deliveryTag);
                }
            } catch (Exception e) {
                LOGGER.error("Problem with not acknowledge exchangeName='{}', routing key ='{}', channel = {} deliveryTag = {}, multiple = {}",
                        exchangeName, routingKey, channelNumber, deliveryTag, multiple, e);
            }
        });
    }

    private void notAcknowledge(Map<Long, SendRequest> sequenceToSendRequest, int channelNumber, long deliveryTag) {
        SendRequest removedRequest = sequenceToSendRequest.remove(deliveryTag);
        if (removedRequest == null) {
            LOGGER.error("Unexpected not acknowledgment exchangeName='{}', routing key ='{}', channel = {} deliveryTag = {}", exchangeName, routingKey, channelNumber, deliveryTag);
            return;
        }

        manualResetEvent.reset();

        if (rejectedSends.add(removedRequest.sendData)) {
            LOGGER.warn("Send data added to rejected list exchangeName='{}', routing key ='{}', data = {}", exchangeName, routingKey, removedRequest.sendData);
        }

        publish(removedRequest.sendData, removedRequest.attempt + 1);
    }

    protected abstract Counter getDeliveryCounter();

    protected abstract Counter getContentCounter();

    protected abstract int extractCountFrom(T message);

    protected abstract String toShortDebugString(T value);

    protected abstract byte[] valueToBytes(T value);

    private void checkPrevious(SendRequest expected, SendRequest actual) {
        if (!Objects.equals(expected, actual)) {
            LOGGER.warn("Previous send request is unexpected, expected '{}' actual '{}'", expected, actual);
        }
    }

    private void waitOne() throws InterruptedException {
        while (!manualResetEvent.waitOne(1, TimeUnit.SECONDS)) {
            LOGGER.warn("Send operation is blocked, exchangeName='{}', routing key ='{}'", exchangeName, routingKey);
        }
    }

    private long getNextDelay(long attempt) {
        if (attempt == 1) {
            return 0;
        }
        long delay = (long)(Math.E * attempt * resendMessageConfiguration.getMinDelay());
        return resendMessageConfiguration.getMaxDelay() > 0 ? Math.min(delay, resendMessageConfiguration.getMaxDelay()) : delay;
    }

    @FunctionalInterface
    private interface ChannelAction {
        void apply(Channel channel) throws Exception;
    }

    private interface ConfirmAction {
        void apply(int channelNumber, long deliveryTag, boolean multiple);
    }

    private static class SenderConfirmListener implements ConfirmListener {

        private final int channelNumber;
        private final ConfirmAction ack;
        private final ConfirmAction nack;

        private SenderConfirmListener(int channelNumber, ConfirmAction ack, ConfirmAction nack) {
            this.channelNumber = channelNumber;
            this.ack = ack;
            this.nack = nack;
        }

        @Override
        public void handleAck(long deliveryTag, boolean multiple) {
            ack.apply(channelNumber, deliveryTag, multiple);
        }

        @Override
        public void handleNack(long deliveryTag, boolean multiple) {
            nack.apply(channelNumber, deliveryTag, multiple);
        }
    }

    private static class SendRequest {
        private final long id = SEND_DATA_REQUEST_COUNTER.incrementAndGet();
        private final SendData sendData;
        private final int attempt;

        private SendRequest(SendData sendData, int attempt) {
            this.sendData = sendData;
            this.attempt = attempt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SendRequest sendRequest = (SendRequest)o;

            return new EqualsBuilder().append(id, sendRequest.id).isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(id).toHashCode();
        }
    }

    private static class SendData {
        private final int size;
        private final byte[] data;
        private final CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        private SendData(byte[] data, int size) {
            this.data = data;
            this.size = size;
        }

        @Override
        public String toString() {
            return "SendData{" +
                    "size=" + size +
                    ", data=" + Arrays.toString(data) +
                    '}';
        }
    }

    private static class ManualResetEvent {
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition condition = lock.newCondition();
        private volatile boolean isOpen;

        public ManualResetEvent(boolean open) {
            isOpen = open;
        }

        public boolean waitOne(long time, TimeUnit unit) throws InterruptedException {
            if (!isOpen) {
                try {
                    lock.lock();
                    if (!isOpen) {
                        //noinspection ResultOfMethodCallIgnored
                        condition.await(time, unit);
                    }
                } finally {
                    lock.unlock();
                }
            }

            return isOpen;
        }

        public void set() {
            if (!isOpen) {
                try {
                    lock.lock();
                    if (!isOpen) {
                        isOpen = true;
                        condition.signalAll();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        public void reset() {
            isOpen = false;
        }
    }
}
