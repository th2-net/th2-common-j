/******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.common.event.bean;

import com.exactpro.th2.common.event.IBodyData;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class TreeTable implements IBodyData {
    public static final String TYPE = "treeTable";

    private final String type = TYPE;
    private final String name;
    private final Map<String, TreeTableEntry> rows;

    public TreeTable(String name, String type, Map<String, TreeTableEntry> rows) {
        if (name != null && name.isBlank()) {
            throw new IllegalArgumentException("Tree table name cannot be empty or blank");
        }
        this.name = name;
        this.rows = rows;
    }

    public TreeTable(String type, Map<String, TreeTableEntry> rows) {
        this(null, TYPE, rows);
    }

    public String getType() {
        return type;
    }

    public Map<String, TreeTableEntry> getRows() {
        return rows;
    }

    public String getName() {
        return name;
    }
}
