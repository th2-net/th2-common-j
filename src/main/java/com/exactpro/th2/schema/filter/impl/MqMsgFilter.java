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
import com.exactpro.th2.schema.message.configuration.MessageRouterConfiguration;
import com.google.protobuf.Message;

public class MqMsgFilter implements Filter {

    private final FilterStrategy filterStrategy;

    private final MessageRouterConfiguration configuration;


    public MqMsgFilter(MessageRouterConfiguration configuration) {
        this(configuration, new DefaultFilterStrategy());
    }

    public MqMsgFilter(MessageRouterConfiguration configuration, FilterStrategy filterStrategy) {
        this.configuration = configuration;
        this.filterStrategy = filterStrategy;
    }


    @Override
    public String check(Message message) {
        var queueAliasResult = "";

        for (var queueEntry : configuration.getQueues().entrySet()) {

            var queueAlias = queueEntry.getKey();
            var queueConfig = queueEntry.getValue();

            var filterResult = filterStrategy.verify(message, queueConfig.getFilters());

            if (filterResult) {
                if (queueAliasResult.isEmpty()) {
                    queueAliasResult = queueAlias;
                } else {
                    throw new FilterCheckException("Two queues match one message " +
                            "according to configuration filters");
                }
            }

        }

        if (queueAliasResult.isEmpty()) {
            throw new FilterCheckException("No filters correspond to message: " + message);
        }

        return queueAliasResult;
    }

}
