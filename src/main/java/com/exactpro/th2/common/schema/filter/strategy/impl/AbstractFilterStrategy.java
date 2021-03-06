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

package com.exactpro.th2.common.schema.filter.strategy.impl;

import com.exactpro.th2.common.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.google.protobuf.Message;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class AbstractFilterStrategy<T extends Message> implements FilterStrategy<T> {

    @Override
    public boolean verify(T message, RouterFilter routerFilter) {

        var msgFieldFilters = new HashMap<>(routerFilter.getMessage());

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


    private boolean checkValues(Map<String, String> messageFields, Map<String, FieldFilterConfiguration> fieldFilters) {
        return fieldFilters.entrySet().stream().allMatch(entry -> {
            var fieldName = entry.getKey();
            var fieldFilter = entry.getValue();
            var msgFieldValue = messageFields.get(fieldName);
            return checkValue(msgFieldValue, fieldFilter);
        });
    }

    private boolean checkValue(String value1, FieldFilterConfiguration filterConfiguration) {
        if (StringUtils.isEmpty(value1)) {
            return false;
        }

        var value2 = filterConfiguration.getValue();

        switch (filterConfiguration.getOperation()) {
            case EQUAL:
                return value1.equals(value2);
            case NOT_EQUAL:
                return !value1.equals(value2);
            case EMPTY:
                return StringUtils.isEmpty(value1);
            case NOT_EMPTY:
                return StringUtils.isNotEmpty(value1);
            default:
                return false;
        }
    }

}
