package com.exactpro.th2.common.tmp;

import java.util.List;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.tmp.impl.ParsedMessageBuilder;

public class Test {
    public static void main(String[] args) {
        MessageFactory factory = new MessageFactory();
        ParsedMessageBuilder message = factory.createParsedMessage();
        message.metadataBuilder()
                .setSessionAlias("test")
                .setDirection(Direction.SECOND)
                .setSequence(1L);
        message.putSimpleField("A", 5)
                .putSimpleField("B", List.of(5, 42))
                // sets message to a field
                .putMessage("C", msg ->
                        msg.putSimpleField("A", 42)
                                .putSimpleField("B", List.of(42, 3))
                                .putMessage("C", inner ->
                                        inner.putSimpleField("A", 5)
                                                .putSimpleField("B", 4)
                                )
                                .putMessages("D", List.of(
                                        inner -> inner.putSimpleField("A", 1),
                                        inner -> inner.putSimpleField("A", 2),
                                        inner -> inner.putSimpleField("A", 3)
                                ))
                )

                // adds message to a collection for the field
                .addMessage("D", msg -> msg.putSimpleField("A", 1))
                .addMessage("D", msg -> msg.putSimpleField("A", 2))
                .addMessage("D", msg -> msg.putSimpleField("A", 3))

                // sets collection to the field
                .putMessages("E", List.of(
                        msg -> msg.putSimpleField("A", 1),
                        msg -> msg.putSimpleField("A", 2)
                ));

        Message protoMessage = message.build();
        System.out.println(MessageUtils.toJson(protoMessage, false));
    }
}
