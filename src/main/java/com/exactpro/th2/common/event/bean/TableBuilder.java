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
package com.exactpro.th2.common.event.bean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

public class TableBuilder<T extends IRow> {
    public static final String TABLE_TYPE = "table";

    private final Collection<T> rows = new ArrayList<>();

    public TableBuilder<T> row(T row) {
        rows.add(row);
        return this;
    }

    public Table build() {
        Table table = new Table();
        table.setType(TABLE_TYPE);
        table.setRows(rows.stream()
                .map(item -> (IRow) item)
                .collect(Collectors.toList()));
        return table;
    }
}
