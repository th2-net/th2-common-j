/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import java.util.StringJoiner;

import com.rabbitmq.client.AMQP.BasicProperties;

class PublicationHolder {
    private final String exchange;
    private final String routingKey;
    private final BasicProperties props;
    private final byte[] body;

    PublicationHolder(String exchange, String routingKey, BasicProperties props, byte[] body) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.props = props;
        this.body = body;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public BasicProperties getProps() {
        return props;
    }

    public byte[] getBody() {
        return body;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", "Publication[", "]")
                .add("exchange='" + exchange + "'")
                .add("routingKey='" + routingKey + "'")
                .add("bodySize='" + body.length + "'")
                .toString();
    }
}
