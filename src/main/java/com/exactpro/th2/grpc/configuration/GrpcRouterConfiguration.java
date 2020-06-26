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
package com.exactpro.th2.grpc.configuration;

import com.exactpro.th2.common.message.configuration.FilterConfiguration;
import com.exactpro.th2.exception.NoConnectionToSendException;
import com.exactpro.th2.grpc.router.strategy.fieldExtraction.FieldExtractionStrategy;
import com.exactpro.th2.grpc.router.strategy.fieldExtraction.impl.Th2MsgFieldExtraction;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Message;
import io.grpc.stub.AbstractStub;
import lombok.Data;

import java.util.Map;

@Data
public class GrpcRouterConfiguration {

    @JsonProperty
    private Map<Class<?>, Class<? extends AbstractStub>> services;

    @JsonProperty
    private Map<String, GrpcConfiguration> servers;

    @JsonProperty
    private Map<String, GrpcRouterFilterConfiguration> filters;

    @JsonProperty
    private Map<String, String> grpcFilters;

    @JsonProperty
    private GrpcConfiguration serverConfiguration;


    public GrpcConfiguration getGrpcConfigByFilter(Message message) {
        return getGrpcConfigByFilter(message, new Th2MsgFieldExtraction());
    }

    public GrpcConfiguration getGrpcConfigByFilter(Message message, FieldExtractionStrategy fieldExtStrategy) {

        var msgFields = fieldExtStrategy.getFields(message);

        return filters.entrySet().stream()
                .filter(entry -> applyFilter(msgFields, entry.getValue().getMessage()))
                .map(entry -> servers.get(grpcFilters.get(entry.getKey())))
                .findFirst()
                .orElseThrow(() ->
                        new NoConnectionToSendException("No grpc connections matching the specified filters")
                );
    }


    private boolean applyFilter(Map<String, String> messageFields, Map<String, FilterConfiguration> fieldFilters) {
        return fieldFilters.entrySet().stream().allMatch(entry -> {
            var fieldName = entry.getKey();
            var fieldFilter = entry.getValue();
            var msgFieldValue = messageFields.get(fieldName);
            return fieldFilter.checkValue(msgFieldValue);
        });
    }

}
