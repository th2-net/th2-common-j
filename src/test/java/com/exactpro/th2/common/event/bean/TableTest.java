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
import com.exactpro.th2.common.event.bean.builder.TableBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TableTest extends BaseTest {

    @Test
    public void testSerializationTable() throws IOException {

        IRow row1 = new IRow() {
            public final String firstColumn = "Column / Row - A1";
            public final String secondColumn = "Column / Row - B1";
        };

        IRow row2 = new IRow() {
            public final String firstColumn = "Column / Row - A2";
            public final String secondColumn = "Column / Row - B2";
        };

        TableBuilder<IRow> tableBuilder = new TableBuilder<>();
        Table table = tableBuilder.row(row1)
                .row(row2).build();
        com.exactpro.th2.infra.grpc.Event event =
                Event.start().bodyData(table).toProtoEvent("id");

        String expectedJson = "[\n" +
                "  {\n" +
                "    \"type\": \"table\",\n" +
                "    \"rows\": [\n" +
                "      {\n" +
                "        \"firstColumn\": \"Column / Row - A1\",\n" +
                "        \"secondColumn\": \"Column / Row - B1\" \n" +
                "      },\n" +
                "      {\n" +
                "        \"firstColumn\": \"Column / Row - A2\",\n" +
                "        \"secondColumn\": \"Column / Row - B2\" \n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "]";

        assertCompareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }

}
