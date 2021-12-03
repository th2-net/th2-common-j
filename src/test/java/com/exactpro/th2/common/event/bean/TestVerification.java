/*
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
 */
package com.exactpro.th2.common.event.bean;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static com.exactpro.th2.common.event.EventUtils.toEventID;

public class TestVerification extends BaseTest {
    private static final String BOOK_NAME = new BoxConfiguration().getBookName();
    
    @Test
    public void testSerializationSimpleVerification() throws IOException {
        Verification verification = new Verification();
        verification.setType("verification");

        VerificationEntry verificationEntry = new VerificationEntry();
        verificationEntry.setType("field");
        verificationEntry.setOperation("NOT_EMPTY");
        verificationEntry.setStatus(VerificationStatus.PASSED);
        verificationEntry.setKey(false);
        verificationEntry.setActual("value");
        verificationEntry.setExpected("*");


        verification.setFields(new HashMap<>() {{
            put("Field A", verificationEntry);
        }});

        com.exactpro.th2.common.grpc.Event event =
                Event.start().bodyData(verification).bookName(BOOK_NAME).toProto(toEventID(BOOK_NAME, "id"));

        String expectedJson = "[\n" +
                "  {\n" +
                "    \"type\": \"verification\",\n" +
                "    \"fields\": {\n" +
                "      \"Field A\": {\n" +
                "        \"type\": \"field\",\n" +
                "        \"operation\": \"NOT_EMPTY\",\n" +
                "        \"status\": \"PASSED\",\n" +
                "        \"key\": false,\n" +
                "        \"actual\": \"value\",\n" +
                "        \"expected\": \"*\"\n" +
                "      }" +
                "   }" +
                "}" +
                "]";

        assertCompareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }

    @Test
    public void testSerializationRecursiveVerification() throws IOException {
        Verification verification = new Verification();
        verification.setType("verification");

        VerificationEntry verificationEntry = new VerificationEntry();
        verificationEntry.setType("collection");
        verificationEntry.setOperation("EQUAL");
        verificationEntry.setKey(false);
        verificationEntry.setActual("1");
        verificationEntry.setExpected("1");
        verificationEntry.setHint("Hint for collection");

        VerificationEntry verificationEntryInner = new VerificationEntry();
        verificationEntryInner.setType("field");
        verificationEntryInner.setOperation("NOT_EQUAL");
        verificationEntryInner.setStatus(VerificationStatus.FAILED);
        verificationEntryInner.setKey(false);
        verificationEntryInner.setActual("9");
        verificationEntryInner.setExpected("9");
        verificationEntryInner.setHint("Hint for inner entry");

        verificationEntry.setFields(new HashMap<>() {{
            put("Field C", verificationEntryInner);
        }});

        verification.setFields(new HashMap<>() {{
            put("Sub message A", verificationEntry);
        }});

        com.exactpro.th2.common.grpc.Event event =
                Event.start().bodyData(verification).bookName(BOOK_NAME).toProto(toEventID(BOOK_NAME, "id"));

        String expectedJson = "[\n" +
                "  {\n" +
                "    \"type\": \"verification\",\n" +
                "    \"fields\": {\n" +
                "      \"Sub message A\": {\n" +
                "        \"type\": \"collection\",\n" +
                "        \"operation\": \"EQUAL\",\n" +
                "        \"key\": false,\n" +
                "        \"actual\": \"1\",\n" +
                "        \"expected\": \"1\",\n" +
                "        \"hint\": \"Hint for collection\",\n" +
                "        \"fields\": {\n" +
                "          \"Field C\": {\n" +
                "            \"type\": \"field\",\n" +
                "            \"operation\": \"NOT_EQUAL\",\n" +
                "            \"status\": \"FAILED\",\n" +
                "            \"key\": false,\n" +
                "            \"actual\": \"9\",\n" +
                "            \"expected\": \"9\",\n" +
                "            \"hint\": \"Hint for inner entry\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "]";

        assertCompareBytesAndJson(event.getBody().toByteArray(), expectedJson);
    }
}
