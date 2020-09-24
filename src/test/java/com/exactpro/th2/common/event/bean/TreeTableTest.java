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

import com.exactpro.th2.common.event.Event;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;


public class TreeTableTest extends BaseTest {

    @Test
    public void testSerializationRow() throws IOException {

        IColumn column = new IColumn() {
            public final String firstColumn = "some text (1)";
            public final String secondColumn = "some text (2)";
        };
        Row row = new Row("row", column);

        TreeTableBuilder<TreeTableEntry> treeTableBuilder = new TreeTableBuilder<>();
        TreeTable treeTable = treeTableBuilder.row("FirstRow", row).build();
        com.exactpro.th2.infra.grpc.Event event =
                Event.start().bodyData(treeTable).toProtoEvent("id");

        String expectedJson = "[{\n" +
                "    \"type\": \"treeTable\",\n" +
                "    \"rows\": {\n" +
                "      \"FirstRow\": {\n" +
                "        \"type\": \"row\",\n" +
                "        \"columns\": {\n" +
                "          \"firstColumn\": \"some text (1)\",\n" +
                "          \"secondColumn\": \"some text (2)\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }]";

        compareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }


    @Test
    public void testSerializationCollection() throws IOException {
        Collection collection = new Collection("collection");
        IColumn column = new IColumn() {
            public final String firstColumn = "some text (BA1)";
            public final String secondColumn = "some text (BA2)";
        };
        Row rowBA = new Row("row", column);

        IColumn columnBB = new IColumn() {
            public final String firstColumn = "some text (BB1)";
            public final String secondColumn = "some text (BB2)";
        };
        Row rowBB = new Row("row", columnBB);

        collection.setRows(new HashMap<>() {{
            put("Row BA", rowBA);
            put("Row BB", rowBB);
        }});


        TreeTableBuilder<TreeTableEntry> treeTableBuilder = new TreeTableBuilder<>();
        TreeTable treeTable = treeTableBuilder.row("Row B with some other name", collection).build();

        com.exactpro.th2.infra.grpc.Event event =
                Event.start().bodyData(treeTable).toProtoEvent("id");

        String expectedJson = "[ {\"type\": \"treeTable\",\n" +
                "               \"rows\": {" +
                "\"Row B with some other name\": {\n" +
                "        \"type\": \"collection\",\n" +
                "        \"rows\": {\n" +
                "          \"Row BA\": {\n" +
                "            \"type\": \"row\",\n" +
                "            \"columns\": {\n" +
                "              \"firstColumn\": \"some text (BA1)\",\n" +
                "              \"secondColumn\": \"some text (BA2)\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"Row BB\": {\n" +
                "            \"type\": \"row\",\n" +
                "            \"columns\": {\n" +
                "              \"firstColumn\": \"some text (BB1)\",\n" +
                "              \"secondColumn\": \"some text (BB2)\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }}}]";

        compareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }


    @Test
    public void testSerializationHybrid() throws IOException {
        Collection collection = new Collection("collection");
        IColumn column = new IColumn() {
            public final String firstColumn = "some text (BA1)";
            public final String secondColumn = "some text (BA2)";
        };

        Row rowBA = new Row("row", column);

        IColumn columnBB = new IColumn() {
            public final String firstColumn = "some text (BB1)";
            public final String secondColumn = "some text (BB2)";
        };
        Row rowBB = new Row("row", columnBB);

        collection.setRows(new HashMap<>() {{
            put("Row BA", rowBA);
            put("Row BB", rowBB);
        }});


        IColumn columnSimple = new IColumn() {
            public final String firstColumn = "some text (1)";
            public final String secondColumn = "some text (2)";
        };
        Row row = new Row("row", columnSimple);

        TreeTableBuilder<TreeTableEntry> treeTableBuilder = new TreeTableBuilder<>();

        TreeTable treeTable = treeTableBuilder.row("Row B with some other name", collection)
                .row("FirstRow", row)
                .build();

        com.exactpro.th2.infra.grpc.Event event =
                Event.start().bodyData(treeTable).toProtoEvent("id");

        String expectedJson = "[ {\"type\": \"treeTable\",\n" +
                "               \"rows\": {" +
                "        \"FirstRow\": {\n" +
                "        \"type\": \"row\",\n" +
                "        \"columns\": {\n" +
                "          \"firstColumn\": \"some text (1)\",\n" +
                "          \"secondColumn\": \"some text (2)\"\n" +
                "        }\n" +
                "      },\n" +
                "\"Row B with some other name\": {\n" +
                "        \"type\": \"collection\",\n" +
                "        \"rows\": {\n" +
                "          \"Row BA\": {\n" +
                "            \"type\": \"row\",\n" +
                "            \"columns\": {\n" +
                "              \"firstColumn\": \"some text (BA1)\",\n" +
                "              \"secondColumn\": \"some text (BA2)\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"Row BB\": {\n" +
                "            \"type\": \"row\",\n" +
                "            \"columns\": {\n" +
                "              \"firstColumn\": \"some text (BB1)\",\n" +
                "              \"secondColumn\": \"some text (BB2)\"\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      }}}]";

        compareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }


    @Test
    public void testSerializationRecursive() throws IOException {
        Collection collection = new Collection("collection");

        IColumn column = new IColumn() {
            public final String firstColumn = "some text (BA1)";
            public final String secondColumn = "some text (BA2)";
        };
        Row rowBA = new Row("row", column);

        collection.setRows(new HashMap<>() {{
            put("Row BA", rowBA);
        }});


        TreeTableBuilder<TreeTableEntry> treeTableBuilder = new TreeTableBuilder<>();
        TreeTable treeTable = treeTableBuilder.row("Row B with some other name", collection).build();

        com.exactpro.th2.infra.grpc.Event event =
                Event.start().bodyData(treeTable).toProtoEvent("id");

        String expectedJson = "[ {\"type\": \"treeTable\",\n" +
                "               \"rows\": {" +
                "\"Row B with some other name\": {\n" +
                "        \"type\": \"collection\",\n" +
                "        \"rows\": {\n" +
                "          \"Row BA\": {\n" +
                "            \"type\": \"row\",\n" +
                "            \"columns\": {\n" +
                "              \"firstColumn\": \"some text (BA1)\",\n" +
                "              \"secondColumn\": \"some text (BA2)\"\n" +
                "            }\n" +
                "          }\n" +
                "      }}}}]";

        compareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }
}
