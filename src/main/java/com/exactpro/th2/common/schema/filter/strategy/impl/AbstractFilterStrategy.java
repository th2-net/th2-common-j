/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.filter.strategy.impl;

import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.google.protobuf.Message;
import org.apache.commons.collections4.MultiMapUtils;
import org.apache.commons.collections4.MultiValuedMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public abstract class AbstractFilterStrategy<T extends Message> implements FilterStrategy<T> {

    private final FieldValueChecker valueChecker = new FieldValueChecker();

    @Override
    public boolean verify(T message, RouterFilter routerFilter) {

        MultiValuedMap<String, FieldFilterConfiguration> msgFieldFilters = MultiMapUtils.newListValuedHashMap();
        msgFieldFilters.putAll(routerFilter.getMessage());
        msgFieldFilters.putAll(routerFilter.getMetadata());

        return checkValues(getFields(message), msgFieldFilters);
    }

    @Override
    public boolean verify(T message, List<? extends RouterFilter> routerFilters) {
        for (var fieldsFilter : routerFilters) {
            if (verify(message, fieldsFilter)) {
                return true;
            }
        }

        return false;
    }

    protected abstract Map<String, String> getFields(T message);

    private boolean checkValues(Map<String, String> messageFields, MultiValuedMap<String, FieldFilterConfiguration> fieldFilters) {
        return fieldFilters.isEmpty() || fieldFilters.keys().stream().anyMatch(fieldName -> {
            String messageValue = messageFields.get(fieldName);
            Collection<FieldFilterConfiguration> filters = fieldFilters.get(fieldName);
            return !filters.isEmpty() && filters.stream().allMatch(filter -> valueChecker.check(messageValue, filter));
        });
    }
}
