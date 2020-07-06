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

import java.util.List;

import com.exactpro.th2.schema.exception.FilterCheckException;
import com.exactpro.th2.schema.filter.AbstractMsgFilter;
import com.exactpro.th2.schema.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.schema.message.configuration.RouterFilterConfiguration;
import com.exactpro.th2.schema.strategy.fieldExtraction.FieldExtractionStrategy;
import com.exactpro.th2.schema.strategy.fieldExtraction.impl.Th2BatchMsgFieldExtraction;
import com.google.protobuf.Message;

public class MqMsgFilter extends AbstractMsgFilter {

    private MessageRouterConfiguration configuration;


    public MqMsgFilter(MessageRouterConfiguration configuration) {
        this.configuration = configuration;
    }


    @Override
    public String check(Message message) {
        return check(message, new Th2BatchMsgFieldExtraction());
    }

    @Override
    public String check(Message message, FieldExtractionStrategy strategy) {
        var queueAliasResult = "";

        for (var queueEntry : configuration.getQueues().entrySet()) {

            var queueAlias = queueEntry.getKey();
            var queueConfig = queueEntry.getValue();

            var filterResult = filter(message, queueConfig.getFilters(), strategy);

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


    private boolean filter(
            Message message,
            List<RouterFilterConfiguration> filtersConfig,
            FieldExtractionStrategy strategy
    ) {
        for (var fieldsFilter : filtersConfig) {

            var msgFieldsFilter = fieldsFilter.getMessage();
            var msgMetadataFilter = fieldsFilter.getMetadata();

            msgFieldsFilter.putAll(msgMetadataFilter);

            if (checkValues(strategy.getFields(message), msgFieldsFilter)) {
                return true;
            }
        }

        return false;
    }

}
