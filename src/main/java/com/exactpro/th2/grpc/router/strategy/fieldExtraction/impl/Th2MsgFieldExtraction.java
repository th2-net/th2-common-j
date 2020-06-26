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
package com.exactpro.th2.grpc.router.strategy.fieldExtraction.impl;

import com.exactpro.th2.grpc.router.strategy.fieldExtraction.FieldExtractionStrategy;
import com.google.protobuf.MapField;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.util.*;

/**
 * The strategy for extracting fields from a message is as follows:
 * assume that the provided object contains message object with name 'message_'
 * and it in turn contains {@link Map}<{@link String},{@link Value}> named 'fields_',
 * this {@link Map} with the conversion of {@link Value}(nested {@code kind_} field) to {@link String} will be returned
 */
public class Th2MsgFieldExtraction implements FieldExtractionStrategy {

    private static final String FIELD_SUFFIX = "_";


    @Override
    public Map<String, String> getFields(Message message) {
        return getFieldsRec(message, new ArrayDeque<>(List.of(
                "message", "fields", "kind"
        )));
    }


    private Map<String, String> getFieldsRec(Message message, Queue<String> fieldNames) {
        if (Objects.isNull(fieldNames) || fieldNames.isEmpty()) {
            return new HashMap<>();
        }

        var fieldName = fieldNames.peek();

        if (Objects.isNull(fieldName)) {
            return new HashMap<>();
        }

        for (var entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getName().equals(fieldName)) {
                var value = entry.getValue();
                if (value instanceof Message) {
                    fieldNames.poll();
                    return getFieldsRec((Message) value, fieldNames);
                } else {
                    return extractFields(message, fieldNames);
                }
            }
        }

        throw new IllegalStateException("No fields were found according to the specified names sequence");

    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private Map<String, String> extractFields(Object message, Queue<String> fieldNames) {
        if (fieldNames.size() < 2) {
            return new HashMap<>();
        }

        addSuffix(fieldNames);

        Map<String, String> map = new HashMap<>();

        var mapField = getField(message, fieldNames.poll());

        var fields = (MapField<String, Object>) mapField.get(message);

        var fieldMap = fields.getMap();

        var valueFieldName = fieldNames.poll();

        for (var entry : fieldMap.entrySet()) {
            var s = entry.getKey();
            var o = entry.getValue();

            map.put(s, getField(o, valueFieldName).get(o).toString());
        }

        return map;
    }

    private Field getField(Object object, String fieldName) throws NoSuchFieldException {
        var field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    private void addSuffix(Queue<String> fieldNames) {
        for (var name : fieldNames) {
            fieldNames.remove(name);
            fieldNames.add(name + FIELD_SUFFIX);
        }
    }

}
