package com.exactpro.th2.common.tmp;

import com.exactpro.th2.common.tmp.impl.ParsedMessageBuilder;

public class MessageFactory {
    public ParsedMessageBuilder createParsedMessage() {
        return new ParsedMessageBuilder();
    }
}
