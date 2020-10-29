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

package com.exactpro.th2.common.schema.strategy.fieldExtraction;


import com.exactpro.th2.common.grpc.Message;

import java.util.Map;
import java.util.stream.Collectors;


public abstract class AbstractTh2MsgFieldExtraction implements FieldExtractionStrategy {

    public static final String SESSION_ALIAS_KEY = "session_alias";
    public static final String MESSAGE_TYPE_KEY = "message_type";
    public static final String DIRECTION_KEY = "direction";


    public Map<String, String> getFields(com.google.protobuf.Message message) {
        var th2Msg = parseMessage(message);

        var messageID = th2Msg.getMetadata().getId();

        var messageFields = th2Msg.getFieldsMap().entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().getSimpleValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        var metadataMsgFields = Map.of(
                SESSION_ALIAS_KEY, messageID.getConnectionId().getSessionAlias(),
                MESSAGE_TYPE_KEY, th2Msg.getDescriptorForType().getName(),
                DIRECTION_KEY, messageID.getDirection().name()
        );

        messageFields.putAll(metadataMsgFields);

        return messageFields;
    }

    public abstract Message parseMessage(com.google.protobuf.Message message);

}
