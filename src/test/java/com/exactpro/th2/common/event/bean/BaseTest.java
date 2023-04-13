/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.time.Instant;

import static com.exactpro.th2.common.event.EventUtils.toEventID;
import static com.fasterxml.jackson.module.kotlin.ExtensionsKt.jacksonObjectMapper;

public class BaseTest {
    public static final BoxConfiguration BOX_CONFIGURATION = new BoxConfiguration();
    public static final String BOOK_NAME = BOX_CONFIGURATION.getBookName();
    public static final String SESSION_GROUP = "test-group";
    public static final String SESSION_ALIAS = "test-alias";
    public static final EventID PARENT_EVENT_ID = toEventID(Instant.now(), BOOK_NAME, "id");

    private static final ObjectMapper jacksonMapper = jacksonObjectMapper();

    protected void assertCompareBytesAndJson(byte[] bytes, String jsonString) throws IOException {
        JsonNode deserialize = jacksonMapper.readTree(bytes);
        JsonNode expectedResult = jacksonMapper.readTree(jsonString);
        Assertions.assertEquals(expectedResult, deserialize);
    }
}
