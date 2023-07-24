/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.filter.strategy.impl;

import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;

import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractTh2MsgFilterStrategy extends AbstractFilterStrategy<com.google.protobuf.Message> {

    public static final String BOOK_KEY = "book";
    public static final String SESSION_GROUP_KEY = "session_group";
    public static final String SESSION_ALIAS_KEY = "session_alias";
    public static final String MESSAGE_TYPE_KEY = "message_type";
    public static final String DIRECTION_KEY = "direction";
    public static final String PROTOCOL_KEY = "protocol";

    @Override
    public Map<String, String> getFields(com.google.protobuf.Message message) {
        Message th2Msg = parseMessage(message);

        MessageMetadata metadata = th2Msg.getMetadata();
        MessageID messageID = metadata.getId();

        var messageFields = th2Msg.getFieldsMap().entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().getSimpleValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        String sessionGroup = messageID.getConnectionId().getSessionGroup();
        String sessionAlias = messageID.getConnectionId().getSessionAlias();
        var metadataMsgFields = Map.of(
                BOOK_KEY, messageID.getBookName(),
                SESSION_GROUP_KEY, sessionGroup.isEmpty() ? sessionAlias : sessionGroup,
                SESSION_ALIAS_KEY, sessionAlias,
                MESSAGE_TYPE_KEY, metadata.getMessageType(),
                DIRECTION_KEY, messageID.getDirection().name(),
                PROTOCOL_KEY, metadata.getProtocol()
        );

        messageFields.putAll(metadataMsgFields);

        return messageFields;
    }

    public abstract Message parseMessage(com.google.protobuf.Message message);
}
