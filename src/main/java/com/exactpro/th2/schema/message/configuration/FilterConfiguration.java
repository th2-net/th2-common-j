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
package com.exactpro.th2.schema.message.configuration;

import org.apache.commons.lang3.StringUtils;

import com.exactpro.th2.infra.grpc.FilterOperation;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

@Getter
public class FilterConfiguration implements Configuration {

    @JsonProperty
    private String value;

    @JsonProperty(required = true)
    private FilterOperation operation;

    //TODO: create factory for operations and not use enum
    public boolean checkValue(String value1) {
        if (StringUtils.isEmpty(value1)) {
            return false;
        }

        switch (this.operation) {
            case EQUAL:
                return value1.equals(this.value);
            case NOT_EQUAL:
                return !value1.equals(this.value);
            case EMPTY:
                return StringUtils.isEmpty(value1);
            case NOT_EMPTY:
                return StringUtils.isNotEmpty(value1);
            default:
                return false;
        }
    }
}

