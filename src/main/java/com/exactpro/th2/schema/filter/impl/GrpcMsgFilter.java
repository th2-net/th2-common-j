/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package com.exactpro.th2.schema.filter.impl;

import com.exactpro.th2.schema.exception.FilterCheckException;
import com.exactpro.th2.schema.filter.Filter;
import com.exactpro.th2.schema.filter.strategy.FilterStrategy;
import com.exactpro.th2.schema.filter.strategy.impl.DefaultFilterStrategy;
import com.exactpro.th2.schema.grpc.configuration.GrpcRawFilterStrategy;
import com.google.protobuf.Message;

public class GrpcMsgFilter implements Filter {

    private final FilterStrategy filterStrategy;

    private final GrpcRawFilterStrategy configuration;


    public GrpcMsgFilter(GrpcRawFilterStrategy configuration) {
        this(configuration, new DefaultFilterStrategy());
    }

    public GrpcMsgFilter(GrpcRawFilterStrategy configuration, FilterStrategy filterStrategy) {
        this.configuration = configuration;
        this.filterStrategy = filterStrategy;
    }


    @Override
    public String check(Message message) {
        var endpointAlias = "";

        for (var fieldsFilter : configuration.getFilters()) {
            if (filterStrategy.verify(message, fieldsFilter)) {
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
