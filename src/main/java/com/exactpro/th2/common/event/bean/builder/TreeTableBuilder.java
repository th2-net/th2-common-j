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

import com.exactpro.th2.common.event.bean.TreeTable;
import com.exactpro.th2.common.event.bean.TreeTableEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TreeTableBuilder {
    public static final String TABLE_TYPE = TreeTable.TYPE;

    private final Map<String, TreeTableEntry> rows = new HashMap<>();
    private final String tableName;

    public TreeTableBuilder() {
        this(null);
    }

    public TreeTableBuilder(String tableName) {
        this.tableName = tableName;
    }

    public TreeTableBuilder row(String rowName, TreeTableEntry row) {
        rows.put(rowName, row);
        return this;
    }

    public TreeTable build() {
        return new TreeTable(tableName, TABLE_TYPE, rows.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }
}
