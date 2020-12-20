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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class SubscribeTarget {

    private final String queue;

    private final String routingKey;

    public SubscribeTarget(String routingKey, String queue) {
        this.queue = queue;
        this.routingKey = routingKey;
    }

    public String getQueue() {
        return queue;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("queue", queue)
                .append("routingKey", routingKey)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        SubscribeTarget that = (SubscribeTarget)obj;

        return new EqualsBuilder().append(queue, that.queue).append(routingKey, that.routingKey).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queue).append(routingKey).toHashCode();
    }
}
