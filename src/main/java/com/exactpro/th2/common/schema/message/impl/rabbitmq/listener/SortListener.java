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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.listener;

import com.exactpro.th2.common.schema.message.MessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;


/**
 * Order incoming message from subscribers
 * <p>
 * Using sequence from method getSequence(T) for ordering
 */
public abstract class SortListener<T> implements MessageListener<T> {

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final Logger LOGGER = LoggerFactory.getLogger(SortListener.class);

    private final MessageListener<T> delegate;
    private final Map<String, TreeMap<Long, T>> buffer = new HashMap<>();
    private final int bufferSize;

    private final Map<String, Long> minSeqMap = new HashMap<>();

    /**
     * @param delegate Message listener which get ordering messages
     * @param bufferSize buffer size for each consumer for ordering messages
     */
    public SortListener(MessageListener<T> delegate, int bufferSize) {
        this.delegate = delegate;
        this.bufferSize = bufferSize;
    }

    /**
     * Create with default buffer size = 1024
     * @param delegate Message listener which get ordering messages
     */
    public SortListener(MessageListener<T> delegate) {
        this(delegate, DEFAULT_BUFFER_SIZE);
    }

    @Override
    public void handler(String consumerTag, T message) throws Exception {
        long seq = getSequence(message);

        Queue<T> handled = new LinkedList<>();

        synchronized (buffer) {
            long minSeq = minSeqMap.computeIfAbsent(consumerTag, key -> Long.MIN_VALUE);
            TreeMap<Long, T> messageBuffer = buffer.computeIfAbsent(consumerTag, key -> new TreeMap<>());

            if (minSeq + 1 == seq || minSeq == Integer.MIN_VALUE) {
                minSeq = seq;
                handled.add(message);

                while (messageBuffer.size() > 0 && messageBuffer.firstKey() == minSeq + 1) {
                    minSeq++;
                    handled.add(messageBuffer.pollFirstEntry().getValue());
                }
            } else if (seq > minSeq) {
                if (messageBuffer.size() >= bufferSize) {
                    LOGGER.warn("Buffer for consumer tag '{}' is full.", consumerTag);
                }
                messageBuffer.put(seq, message);
            } else {
                LOGGER.debug("Incoming message has less sequence than already handled.");
            }

            minSeqMap.put(consumerTag, minSeq);
        }

        for (T msg : handled) {
            delegate.handler(consumerTag, msg);
        }
    }

    @Override
    public void onClose() {
        delegate.onClose();
    }

    protected abstract long getSequence(T message);
}
