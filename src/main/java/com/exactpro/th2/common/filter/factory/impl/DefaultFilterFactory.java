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
package com.exactpro.th2.common.filter.factory.impl;

import com.exactpro.th2.common.filter.Filter;
import com.exactpro.th2.common.filter.factory.FilterFactory;
import com.exactpro.th2.common.filter.impl.GrpcMsgFilter;
import com.exactpro.th2.common.filter.impl.MqMsgFilter;
import com.exactpro.th2.common.message.configuration.FilterableConfiguration;
import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.grpc.configuration.GrpcRawStrategy;

public class DefaultFilterFactory implements FilterFactory {

    @Override
    public Filter createFilter(FilterableConfiguration configuration) {
        if (configuration instanceof MessageRouterConfiguration) {
            return new MqMsgFilter((MessageRouterConfiguration) configuration);
        } else if (configuration instanceof GrpcRawStrategy) {
            return new GrpcMsgFilter((GrpcRawStrategy) configuration);
        }

        throw new IllegalStateException("Unknown configuration type");
    }

}
