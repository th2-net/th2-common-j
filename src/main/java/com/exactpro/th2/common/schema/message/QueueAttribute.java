/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

public enum QueueAttribute {
    FIRST("first"),
    SECOND("second"),
    SUBSCRIBE("subscribe"),
    PUBLISH("publish"),
    RAW("raw"),
    PARSED("parsed"),
    STORE("store"),
    EVENT("event"),
    TRANSPORT_GROUP("transport-group");

    private final String value;

    QueueAttribute(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
       return value;
    }
}
