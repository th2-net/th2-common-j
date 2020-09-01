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

package com.exactpro.th2.schema.filter;

import com.exactpro.th2.schema.exception.FilterCheckException;
import com.exactpro.th2.schema.filter.model.FilterResult;
import com.google.protobuf.Message;

public interface Filter {

    /**
     * @param message message whose fields will be filtered
     * @return {@link FilterResult} that represents one filter alias of target entity (queue/endpoint),
     * which correspond to provided message, and set of filter aliases that have no filters
     * @throws FilterCheckException if two filters match the message;
     *                              if none of the filters match the provided message
     *                              and no target entities without filters present
     */
    FilterResult check(Message message);

}
