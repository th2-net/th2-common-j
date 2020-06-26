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
package com.exactpro.th2.common.message.configuration;

import com.exactpro.th2.infra.grpc.FilterOperation;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class FilterConfiguration {

    @JsonProperty
    private String value;

    @JsonProperty(required = true)
    private FilterOperation operation;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public FilterOperation getOperation() {
        return operation;
    }

    public void setOperation(FilterOperation operation) {
        this.operation = operation;
    }


    public boolean checkValue(String value1) {
        if (Objects.isNull(value1)) {
            return false;
        }

        var value2 = this.getValue();
        switch (this.getOperation()) {
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

