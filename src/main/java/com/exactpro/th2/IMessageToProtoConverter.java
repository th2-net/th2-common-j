/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2;

import com.exactpro.th2.infra.grpc.ListValue;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.Value;
import com.exactpro.sf.common.messages.IMessage;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class IMessageToProtoConverter {
    public Message.Builder toProtoMessage(IMessage message) {
        Message.Builder builder = Message.newBuilder();
        builder.getMetadataBuilder().setMessageType(message.getName());
        for (String fieldName : message.getFieldNames()) {
            Object fieldValue = message.getField(fieldName);
            Value convertedValue = convertToValue(fieldValue);
            builder.putFields(fieldName, convertedValue);
        }
        return builder;
    }

    private Value convertToValue(Object fieldValue) {
        Value.Builder valueBuilder = Value.newBuilder();
        if (fieldValue instanceof IMessage) {
            Message nestedMessage = convertComplex((IMessage) fieldValue);
            valueBuilder.setMessageValue(nestedMessage);
        } else if (fieldValue instanceof List<?>) {
            ListValue listValue = convertToListValue(fieldValue);
            valueBuilder.setListValue(listValue);
        } else {
            valueBuilder.setSimpleValue(fieldValue.toString());
        }
        return valueBuilder.build();
    }

    @NotNull
    private ListValue convertToListValue(Object fieldValue) {
        ListValue.Builder listBuilder = ListValue.newBuilder();
        if (((List<?>)fieldValue).get(0) instanceof IMessage) {
            ((List<?>)fieldValue)
                    .stream()
                    .forEach(message -> listBuilder.addValues(
                            Value.newBuilder()
                            .setMessageValue(convertComplex((IMessage)message))
                            .build()
                    ));
        } else {
            ((List<?>)fieldValue).stream()
                    .forEach(value -> listBuilder.addValues(
                            Value.newBuilder().setSimpleValue(value.toString()).build()
                    ));
        }
        return listBuilder.build();
    }

    private Message convertComplex(IMessage fieldValue) {
        IMessage nestedMessage = fieldValue;
        Message.Builder nestedMessageBuilder = Message.newBuilder();
        for (String fieldName : nestedMessage.getFieldNames()) {
            Value convertedValue = convertToValue(nestedMessage.getField(fieldName));
            nestedMessageBuilder.putFields(fieldName, convertedValue);
        }
        return nestedMessageBuilder.build();
    }
}
