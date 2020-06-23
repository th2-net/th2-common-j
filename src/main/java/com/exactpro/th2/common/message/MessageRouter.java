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
package com.exactpro.th2.common.message;

import java.io.IOException;

import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.infra.grpc.MessageFilter;

public interface MessageRouter<T> {

    void init(MessageRouterConfiguration configuration);

    SubscriberMonitor subscribe(MessageFilter filter, MessageListener<T> callback);
    SubscriberMonitor subscribe(String queueAlias, MessageListener<T> callback);
    SubscriberMonitor subscribeAll(MessageListener<T> callback);

    void send(T message) throws IOException;

    void send(String queueAlias, T message) throws IOException;

}
