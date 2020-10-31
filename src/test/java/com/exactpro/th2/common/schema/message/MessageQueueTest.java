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
package com.exactpro.th2.common.schema.message;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.exactpro.th2.common.schema.message.impl.rabbitmq.parsed.RabbitParsedBatchQueue;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.raw.RabbitRawBatchQueue;

//FIXME: it maybe redundant
public class MessageQueueTest {

    @Test
    public void testClassLoading() {
        Set<? extends Class<? extends MessageQueue>> expected = Set.of(RabbitParsedBatchQueue.class, RabbitRawBatchQueue.class);
        Set<? extends Class<? extends MessageQueue>> actual = ServiceLoader.load(MessageQueue.class).stream()
                .map(Provider::type)
                .collect(Collectors.toUnmodifiableSet());

        Assertions.assertEquals(expected, actual);
    }
}