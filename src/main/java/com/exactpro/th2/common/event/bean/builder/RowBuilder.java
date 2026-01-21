/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.event.bean.builder;

import com.exactpro.th2.common.event.bean.IColumn;
import com.exactpro.th2.common.event.bean.Row;

import static java.util.Objects.requireNonNull;

public class RowBuilder {
    public static final String ROW_TYPE = "row";

    private IColumn columns;

    public RowBuilder column(IColumn columns) {
        this.columns = requireNonNull(columns, "Column can't be null");
        return this;
    }

    public Row build() {
       return new Row(ROW_TYPE, columns);
    }
}
