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
package com.exactpro.th2.common.message;

import static java.util.Objects.requireNonNull;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.common.event.bean.MessageID;
import com.exactpro.th2.infra.grpc.ConnectionID;
import com.exactpro.th2.infra.grpc.Direction;

@SuppressWarnings("ClassNamePrefixedWithPackageName")
public class MessageUtils {

    @NotNull
    public static com.exactpro.th2.infra.grpc.MessageID toProtoMessageID(@NotNull MessageID beanMessageID) {
        requireNonNull(beanMessageID, "Bean message id can't be bull");
        return com.exactpro.th2.infra.grpc.MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder()
                        .setSessionAlias(requireNonNull(beanMessageID.getSessionAlias(), "Session alias can't be null"))
                        .build())
                .setDirection(Direction.forNumber(beanMessageID.getDirection()))
                .setSequence(beanMessageID.getSequence())
                .build();
    }

    /**
     * @deprecated {@link MessageID} class is deprecated
     */
    @NotNull
    @Deprecated(since = "TH2 1.1", forRemoval = true)
    public static MessageID toBeanMessageID(@NotNull com.exactpro.th2.infra.grpc.MessageID protoMessageID) {
        requireNonNull(protoMessageID, "Probuf message id can't be bull");
        MessageID messageID = new MessageID();
        messageID.setSessionAlias(requireNonNull(protoMessageID.getConnectionId().getSessionAlias(), "Session alias can't be null"));
        messageID.setDirection(protoMessageID.getDirection().getNumber());
        messageID.setSequence(protoMessageID.getSequence());
        return  messageID;
    }
}
