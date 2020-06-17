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

import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.sf.common.messages.FieldMetaData;
import com.exactpro.sf.common.messages.IFieldInfo;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.MsgMetaData;

import java.util.Set;

public class MessageWrapper implements IMessage {
    private final IMessage source;
    private MessageID messageId;

    public MessageWrapper(IMessage source) {
        this.source = source;
    }

    @Override
    public String getName() {
        return source.getName();
    }

    @Override
    public String getNamespace() {
        return source.getNamespace();
    }

    @Override
    public MsgMetaData getMetaData() {
        return source.getMetaData();
    }

    @Override
    public void addField(String name, Object value) {
        source.addField(name, value);
    }

    @Override
    public Object removeField(String name) {
        return source.removeField(name);
    }

    @Override
    public <T> T getField(String name) {
        return source.getField(name);
    }

    @Override
    public FieldMetaData getFieldMetaData(String name) {
        return source.getFieldMetaData(name);
    }

    @Override
    public boolean isFieldSet(String name) {
        return source.isFieldSet(name);
    }

    @Override
    public Set<String> getFieldNames() {
        return source.getFieldNames();
    }

    @Override
    public int getFieldCount() {
        return source.getFieldCount();
    }

    @Override
    public IFieldInfo getFieldInfo(String name) {
        return source.getFieldInfo(name);
    }

    @Override
    public IMessage cloneMessage() {
        return source.cloneMessage();
    }

    @Override
    public boolean compare(IMessage message) {
        return source.compare(message);
    }

    public MessageID getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageID messageId) {
        this.messageId = messageId;
    }

    @Override
    public String toString() {
        return getName() + " : " + source;
    }
}
