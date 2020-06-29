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
package com.exactpro.th2.common.filter;

import com.exactpro.th2.common.message.configuration.FilterConfiguration;
import com.exactpro.th2.common.strategy.fieldExtraction.FieldExtractionStrategy;
import com.exactpro.th2.configuration.FilterableConfiguration;
import com.exactpro.th2.exception.FilterCheckException;
import com.google.protobuf.Message;

import java.util.Map;
import java.util.Set;


public abstract class AbstractTh2MsgFilter implements Filter {

    private FilterableConfiguration configuration;


    public AbstractTh2MsgFilter(FilterableConfiguration configuration) {
        this.configuration = configuration;
    }


    /**
     * @see AbstractTh2MsgFilter#check(Message, Set)
     */
    @Override
    public Set<String> check(Message message) {
        return check(message, Set.of());
    }

    /**
     * @param message message whose fields will be filtered
     * @return only one filter alias which correspond to provided message
     * @throws FilterCheckException if two filters match the message or none
     *                              of the filters match the provided message
     */
    @Override
    public Set<String> check(Message message, Set<String> exceptFilters) {
        String alias = "";

        var filtersMatch = false;

        var targetFilters = configuration.getFilters();

        targetFilters.keySet().retainAll(exceptFilters);

        for (var fieldsFilter : targetFilters.entrySet()) {

            var msgFieldsFilter = fieldsFilter.getValue().getMessage();
            var msgMetadataFilter = fieldsFilter.getValue().getMetadata();

            msgFieldsFilter.putAll(msgMetadataFilter);

            if (checkValues(getFieldExtStrategy().getFields(message), msgFieldsFilter)) {
                if (filtersMatch) {
                    throw new FilterCheckException("Two filters correspond to one message!");
                }
                filtersMatch = true;
                alias = fieldsFilter.getKey();
            }
        }

        if (alias.isEmpty()) {
            throw new FilterCheckException("No filters correspond to message: " + message);
        }

        return Set.of(alias);
    }

    public abstract FieldExtractionStrategy getFieldExtStrategy();


    private boolean checkValues(Map<String, String> messageFields, Map<String, FilterConfiguration> fieldFilters) {
        return fieldFilters.entrySet().stream().allMatch(entry -> {
            var fieldName = entry.getKey();
            var fieldFilter = entry.getValue();
            var msgFieldValue = messageFields.get(fieldName);
            return fieldFilter.checkValue(msgFieldValue);
        });
    }

}
