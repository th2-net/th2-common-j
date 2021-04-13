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

package com.exactpro.th2.common.schema.strategy.route.json;

import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class JsonSerializerRoutingStrategy extends StdSerializer<RoutingStrategy> {

    private ObjectMapper mapper;

    public JsonSerializerRoutingStrategy() {
        super(RoutingStrategy.class, false);

        mapper = new ObjectMapper();
    }

    @Override
    public void serialize(RoutingStrategy value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        JsonNode node = mapper.valueToTree(value.getConfiguration());
        if (node instanceof ObjectNode) {
            ((ObjectNode) node).put("name", value.getName());
            gen.writeTree(node);
        }
    }
}
