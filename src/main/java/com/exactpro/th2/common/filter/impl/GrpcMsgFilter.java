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
package com.exactpro.th2.common.filter.impl;

import com.exactpro.th2.common.filter.AbstractTh2MsgFilter;
import com.exactpro.th2.common.strategy.fieldExtraction.FieldExtractionStrategy;
import com.exactpro.th2.common.strategy.fieldExtraction.impl.Th2MsgFieldExtraction;
import com.exactpro.th2.configuration.FilterableConfiguration;

public class GrpcMsgFilter extends AbstractTh2MsgFilter {

    public GrpcMsgFilter(FilterableConfiguration configuration) {
        super(configuration);
    }

    @Override
    public FieldExtractionStrategy getFieldExtStrategy() {
        return new Th2MsgFieldExtraction();
    }

}
