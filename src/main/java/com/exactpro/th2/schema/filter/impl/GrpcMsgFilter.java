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
package com.exactpro.th2.schema.filter.impl;

import com.exactpro.th2.schema.exception.FilterCheckException;
import com.exactpro.th2.schema.filter.AbstractMsgFilter;
import com.exactpro.th2.schema.grpc.configuration.GrpcRawStrategy;
import com.exactpro.th2.schema.strategy.fieldExtraction.FieldExtractionStrategy;
import com.exactpro.th2.schema.strategy.fieldExtraction.impl.Th2MsgFieldExtraction;
import com.google.protobuf.Message;

public class GrpcMsgFilter extends AbstractMsgFilter {

    private GrpcRawStrategy configuration;


    public GrpcMsgFilter(GrpcRawStrategy configuration) {
        this.configuration = configuration;
    }


    @Override
    public String check(Message message) {
        return check(message, new Th2MsgFieldExtraction());
    }

    @Override
    public String check(Message message, FieldExtractionStrategy strategy) {
        var endpointAlias = "";

        for (var fieldsFilter : configuration.getFilters()) {

            var msgFieldsFilter = fieldsFilter.getMessage();
            var msgMetadataFilter = fieldsFilter.getMetadata();

            msgFieldsFilter.putAll(msgMetadataFilter);

            if (checkValues(strategy.getFields(message), msgFieldsFilter)) {
                if (!endpointAlias.isEmpty()) {
                    throw new FilterCheckException("Two endpoints match one " +
                            "message according to configuration filters");
                }
                endpointAlias = fieldsFilter.getEndpoint();
            }
        }

        if (endpointAlias.isEmpty()) {
            throw new FilterCheckException("No filters correspond to message: " + message);
        }

        return endpointAlias;
    }

}
