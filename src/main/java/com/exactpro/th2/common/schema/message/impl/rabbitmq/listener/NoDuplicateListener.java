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
package com.exactpro.th2.common.schema.message.impl.rabbitmq.listener;

import java.util.LinkedHashMap;
import java.util.Map.Entry;

import com.exactpro.th2.common.schema.message.MessageListener;

/**
 * Message listener which remove duplicates from subscribers
 * <p>
 * Using buffer of object's hash code for check duplicates
 *
 * @param <T> must override <b>hashCode</b> method
 */
public final class NoDuplicateListener<T> implements MessageListener<T> {

    private static final int DEFAULT_BUFFER_SIZE = 1024;

    private final MessageListener<T> delegate;

    private final int bufferSize;

    private final LinkedHashMap<Integer, Object> buffer = new LinkedHashMap<>(){
        @Override
        protected boolean removeEldestEntry(Entry<Integer, Object> eldest) {
            return size() >= bufferSize;
        }
    };

    private final Object bufferLock = new Object();

    /**
     * @param delegate Message listener which get only unique messages
     * @param bufferSize
     */
    public NoDuplicateListener(MessageListener<T> delegate, int bufferSize) {
        this.delegate = delegate;
        this.bufferSize = bufferSize;
    }

    /**
     * Create with default buffer size = 1024
     * @param delegate Message listener which get only unique messages
     */
    public NoDuplicateListener(MessageListener<T> delegate) {
        this(delegate, DEFAULT_BUFFER_SIZE);
    }

    @Override
    public void handler(String consumerTag, T message) throws Exception {
        int hashCode = message.hashCode();

        synchronized (bufferLock) {
            if (buffer.containsKey(hashCode)) {
                return;
            }

            buffer.put(hashCode, null);
        }

        delegate.handler(consumerTag, message);
    }

    @Override
    public void onClose() {
        delegate.onClose();
    }
}
