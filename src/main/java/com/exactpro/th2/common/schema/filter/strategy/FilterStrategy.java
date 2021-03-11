/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.filter.strategy;

import com.exactpro.th2.common.schema.filter.strategy.impl.DefaultFilterStrategy;
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration;
import com.exactpro.th2.common.schema.message.configuration.RouterFilter;
import com.google.protobuf.Message;

import java.util.List;

public interface FilterStrategy {

    public static final FilterStrategy DEFAULT_FILTER_STRATEGY = new DefaultFilterStrategy();

    /**
     * Checks the provided message against the provided router filter.
     * If all field filters match the message fields ('<b>and</b>' condition),
     * then returns {@code true}, otherwise {@code false}.
     *
     * @param message      message whose fields will be filtered
     * @param routerFilter router filter encapsulating many '{@link FieldFilterConfiguration}' filters
     * @return if all field filters are passed successfully then {@code true}, otherwise {@code false}
     * @see #verify(Message, List)
     */
    boolean verify(Message message, RouterFilter routerFilter);

    /**
     * Checks the provided message against the provided router filters.
     * If at least one {@link RouterFilter} was passed ('<b>or</b>' condition),
     * then returns {@code true}, otherwise {@code false}.
     *
     * @param message       message whose fields will be filtered
     * @param routerFilters router filters encapsulating many field filters
     * @return if at least one router filter passed successfully then {@code true}, otherwise {@code false}
     * @see #verify(Message, RouterFilter)
     */
    boolean verify(Message message, List<? extends RouterFilter> routerFilters);

}
