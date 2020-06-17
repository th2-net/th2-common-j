/******************************************************************************
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
 ******************************************************************************/
package com.exactpro.th2;

import com.exactpro.sf.aml.scriptutil.StaticUtil.IFilter;
import com.exactpro.th2.infra.grpc.ListValue;
import com.exactpro.th2.infra.grpc.ListValueFilter;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageFilter;
import com.exactpro.th2.infra.grpc.Value;
import com.exactpro.th2.infra.grpc.ValueFilter;
import com.exactpro.sf.aml.scriptutil.StaticUtil;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.structures.IDictionaryStructure;
import com.exactpro.sf.common.messages.structures.IFieldStructure;
import com.exactpro.sf.common.messages.structures.IMessageStructure;
import com.exactpro.sf.common.util.StringUtil;
import com.exactpro.sf.comparison.conversion.MultiConverter;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.externalapi.IMessageFactoryProxy;
import com.google.protobuf.InvalidProtocolBufferException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoToIMessageConverter {
    private static final Logger logger = LoggerFactory.getLogger(ProtoToIMessageConverter.class.getName());
    private final IMessageFactoryProxy messageFactory;
    private final IDictionaryStructure dictionary;
    private final SailfishURI dictionaryURI;

    public ProtoToIMessageConverter(IMessageFactoryProxy messageFactory,
                                    IDictionaryStructure dictionaryStructure,
                                    SailfishURI dictionaryURI) {
        this.messageFactory = messageFactory;
        this.dictionary = dictionaryStructure;
        this.dictionaryURI = dictionaryURI;
    }

    public MessageWrapper fromProtoMessage(byte[] messageData, boolean useDictionary) throws InvalidProtocolBufferException {
        Message receivedMessage = Message.parseFrom(messageData);
        return fromProtoMessage(receivedMessage, useDictionary);
    }

    @NotNull
    public MessageWrapper fromProtoMessage(Message receivedMessage, boolean useDictionary) {
        IMessage convertedMessage = useDictionary
                ? convertByDictionary(receivedMessage.getFieldsMap(),
                        dictionary.getMessages().get(receivedMessage.getMetadata().getMessageType()))
                : convertWithoutDictionary(receivedMessage.getFieldsMap(), receivedMessage.getMetadata().getMessageType());
        MessageWrapper messageWrapper = new MessageWrapper(convertedMessage);
        messageWrapper.setMessageId(receivedMessage.getMetadata().getId());
        return messageWrapper;
    }

    public IMessage fromProtoFilter(MessageFilter messageFilter, String messageName) {
        IMessage message = messageFactory.createMessage(dictionaryURI, messageName);
        for (Map.Entry<String, ValueFilter> filterEntry : messageFilter.getFieldsMap().entrySet()) {
            message.addField(filterEntry.getKey(), traverseFilterField(filterEntry.getKey(), filterEntry.getValue()));
        }
        return message;
    }

    private Object traverseFilterField(String fieldname, ValueFilter value) {
        if (value.hasListFilter()) {
            return traverseCollection(fieldname, value.getListFilter());
        }
        if (value.hasMessageFilter()) {
            return fromProtoFilter(value.getMessageFilter(), fieldname);
        }
        return toSimpleFilter(value);
    }

    private Object toSimpleFilter(ValueFilter value) {
        switch (value.getOperation()) {
            case EQUAL:
                return StaticUtil.simpleFilter(0, null, StringUtil.enclose(value.getSimpleFilter(), '\''));
            case NOT_EQUAL:
                // Enclose value to single quotes isn't required for arguments
                return StaticUtil.filter(0, null, "x != value", "value", value.getSimpleFilter());
            case EMPTY:
                return StaticUtil.nullFilter(0, null);
            case NOT_EMPTY:
                return StaticUtil.notNullFilter(0, null);
            default:
                throw new IllegalArgumentException("Unsupported operation " + value.getOperation());
        }
    }

    private Object traverseCollection(String fieldName, ListValueFilter listFilter) {
        return listFilter.getValuesList().stream()
                .map(value -> traverseFilterField(fieldName, value))
                .collect(Collectors.toList());
    }

    private IMessage convertByDictionary(Message message) {
        String messageType = message.getMetadata().getMessageType();
        IMessageStructure messageStructure = dictionary.getMessages().get(messageType);
        return convertByDictionary(message.getFieldsMap(), messageStructure);
    }
    private IMessage convertByDictionary(Map<String, Value> fieldsMap, IFieldStructure messageStructure) {
        IMessage message = messageFactory.createMessage(dictionaryURI, messageStructure.getName());
        for (Map.Entry<String, Value> fieldEntry : fieldsMap.entrySet()) {
            String fieldName = fieldEntry.getKey();
            Value fieldValue = fieldEntry.getValue();
            IFieldStructure fieldStructure = messageStructure.getFields().get(fieldName);
            tryToTraverseField(message, fieldName, fieldValue, fieldStructure);
        }
        logger.debug("Converted message: {}", message);
        return message;
    }

    private IMessage convertWithoutDictionary(Map<String, Value> fieldsMap, String messageType) {
        IMessage message = messageFactory.createMessage(dictionaryURI, messageType);
        for (Map.Entry<String, Value> fieldEntry : fieldsMap.entrySet()) {
            String fieldName = fieldEntry.getKey();
            Value fieldValue = fieldEntry.getValue();
            Object traverseField = traverseField(fieldName, fieldValue);
            message.addField(fieldName, traverseField);
        }
        logger.debug("Converted message: {}", message);
        return message;
    }

    private Object traverseField(String fieldName, Value fieldValue) {
        if (fieldValue.hasMessageValue()) {
            return convertWithoutDictionary(fieldValue.getMessageValue().getFieldsMap(), fieldName);
        } else if (fieldValue.hasListValue()) {
            return convertList(fieldName, fieldValue.getListValue());
        }
        return fieldValue.getSimpleValue();
    }

    private List<?> convertList(String fieldName, ListValue list) {
        return list.getValuesList().stream()
                .map(value -> traverseField(fieldName, value))
                .collect(Collectors.toList());
    }

    private void tryToTraverseField(IMessage message, String fieldName, Value value, IFieldStructure fieldStructure) {
        if (fieldStructure != null) {
            Object convertedValue = fieldStructure.isComplex()
                    ? processComplex(value, fieldStructure)
                    : convertSimple(value, fieldStructure);
            message.addField(fieldName, convertedValue);
        }
    }

    private Object convertSimple(Value value, IFieldStructure fieldStructure) {
        if (fieldStructure.isCollection()) {
            return value.getListValue().getValuesList()
                    .stream()
                    .map(element -> convertToTarget(element, fieldStructure))
                    .collect(Collectors.toList());
        }
        return convertToTarget(value, fieldStructure);
    }

    private Object convertToTarget(Value value, IFieldStructure fieldStructure) {
        try {
            return MultiConverter.convert(value.getSimpleValue(),
                    Class.forName(fieldStructure.getJavaType().value()));
        } catch (ClassNotFoundException e) {
            logger.error("Could not convert {} value", value, e);
            throw new RuntimeException(e);
        }
    }

    private Object processComplex(Value value, IFieldStructure fieldStructure) {
        if (fieldStructure.isCollection()) {
            return convertComplexList(value.getListValue(), fieldStructure);
        }
        return convertByDictionary(value.getMessageValue().getFieldsMap(), fieldStructure);
    }

    private List<IMessage> convertComplexList(ListValue listValue, IFieldStructure fieldStructure) {
        return listValue.getValuesList()
                .stream()
                .map(value -> convertByDictionary(value.getMessageValue()))
                .collect(Collectors.toList());
    }
}
