package com.exactpro.th2.common.schema.message;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatch;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.IMessageFactory;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;

public class RabbitMqTest {
    private static final int GROUPS = 100;
    private static final int MESSAGES_IN_GROUP = 100;

    private static final String RAW_ATTR_1 = "raw-attr-1";
    private static final String RAW_ATTR_2 = "raw-attr-2";

    private static final String PARSED_ATTR_1 = "parsed-attr-1";
    private static final String PARSED_ATTR_2 = "parsed-attr-2";

    private static final String EVENT_ATTR_1 = "event-attr-1";
    private static final String EVENT_ATTR_2 = "event-attr-2";

    private static final String GROUP_BATCH_ATTR_1 = "group-batch-attr-1";
    private static final String GROUP_BATCH_ATTR_2 = "group-batch-attr-2";

    public static void main(String[] args) {
        CommonFactory factory = CommonFactory.createFromArguments("-c", "src/test/resources/test");
        factory.start();

        new Thread(() -> {
            int i = 1;
            while (true) {
                try {
                    sendRaw(factory, i);
                    sendParsed(factory, i);
                    sendEvent(factory, i);
                    sendGroupBatch(factory, i);
                    i++;
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                sleep(3000L);
            }
        }).start();

        new Thread(() -> {
            sleep(300L);
            while (true) {
                subscribeRaw(factory);
                subscribeParsed(factory);
                subscribeEvent(factory);
                subscribeGroupBatch(factory);
                sleep(5000L);
            }
        }).start();
    }

    private static void sendRaw(CommonFactory f, int i) throws IOException {
        f.getMessageRouterRawBatch().send(createRawMessageBatch("raw-" + RAW_ATTR_1 + "-" + i, "alias_1", f), RAW_ATTR_1);
        f.getMessageRouterRawBatch().send(createRawMessageBatch("raw-" + RAW_ATTR_2 + "-" + i, "alias_2", f), RAW_ATTR_2);
        f.getMessageRouterRawBatch().sendAll(createRawMessageBatch("raw-all" + i, "alias_3", f));
    }

    private static void subscribeRaw(CommonFactory f) {
        f.getMessageRouterRawBatch().subscribe((consumerTag, message) -> System.out.println("Raw for " + RAW_ATTR_1 + ":\n" + message), RAW_ATTR_1);
        f.getMessageRouterRawBatch().subscribe((consumerTag, message) -> System.out.println("Raw for " + RAW_ATTR_2 + ":\n" + message), RAW_ATTR_2);
        f.getMessageRouterRawBatch().subscribeAll((consumerTag, message) -> System.out.println("Raw for all:\n" + message));
    }

    private static void sendParsed(CommonFactory f, int i) throws IOException {
        f.getMessageRouterParsedBatch().send(createMessageBatch("parsed-" + PARSED_ATTR_1 + "-" + i, "alias_1", f), PARSED_ATTR_1);
        f.getMessageRouterParsedBatch().send(createMessageBatch("parsed-" + PARSED_ATTR_2 + "-" + i, "alias_2", f), PARSED_ATTR_2);
        f.getMessageRouterParsedBatch().sendAll(createMessageBatch("parsed-all" + i, "alias_3", f));
    }

    private static void subscribeParsed(CommonFactory f) {
        f.getMessageRouterParsedBatch().subscribe((consumerTag, message) -> System.out.println("Parsed for " + PARSED_ATTR_1 + ":\n" + message), PARSED_ATTR_1);
        f.getMessageRouterParsedBatch().subscribe((consumerTag, message) -> System.out.println("Parsed for " + PARSED_ATTR_2 + ":\n" + message), PARSED_ATTR_2);
        f.getMessageRouterParsedBatch().subscribeAll((consumerTag, message) -> System.out.println("Parsed for all:\n" + message));
    }

    private static void sendEvent(CommonFactory f, int i) throws IOException {
        f.getEventBatchRouter().send(createEventBatch("event-" + EVENT_ATTR_1 + "-" + i, f), EVENT_ATTR_1);
        f.getEventBatchRouter().send(createEventBatch("event-" + EVENT_ATTR_2 + "-" + i, f), EVENT_ATTR_2);
        f.getEventBatchRouter().sendAll(createEventBatch("event-all" + i, f));
    }

    private static void subscribeEvent(CommonFactory f) {
        f.getEventBatchRouter().subscribe((consumerTag, message) -> System.out.println("Events for " + EVENT_ATTR_1 + ":\n" + message), EVENT_ATTR_1);
        f.getEventBatchRouter().subscribe((consumerTag, message) -> System.out.println("Events for " + EVENT_ATTR_2 + ":\n" + message), EVENT_ATTR_2);
        f.getEventBatchRouter().subscribeAll((consumerTag, message) -> System.out.println("Events for all:\n" + message));
    }

    private static void sendGroupBatch(CommonFactory f, int i) throws IOException {
        f.getMessageRouterMessageGroupBatch().send(createMessageGroupBatch("group-batch-" + GROUP_BATCH_ATTR_1 + "-" + i, "alias_1", f), GROUP_BATCH_ATTR_1);
        f.getMessageRouterMessageGroupBatch().send(createMessageGroupBatch("group-batch-" + GROUP_BATCH_ATTR_2 + "-" + i, "alias_2", f), GROUP_BATCH_ATTR_2);
        f.getMessageRouterMessageGroupBatch().sendAll(createMessageGroupBatch("group-batch-all" + i, "alias_3", f));
    }

    private static void subscribeGroupBatch(CommonFactory f) {
        f.getMessageRouterMessageGroupBatch().subscribe((consumerTag, message) -> System.out.println("Group batch for " + GROUP_BATCH_ATTR_1 + ":\n" + message), GROUP_BATCH_ATTR_1);
        f.getMessageRouterMessageGroupBatch().subscribe((consumerTag, message) -> System.out.println("Group batch for " + GROUP_BATCH_ATTR_2 + ":\n" + message), GROUP_BATCH_ATTR_2);
        f.getMessageRouterMessageGroupBatch().subscribeAll((consumerTag, message) -> System.out.println("Group batch for all:\n" + message));
    }

    private static RawMessageBatch createRawMessageBatch(String name, String sessionAlias, CommonFactory factory) {
        RawMessageBatch.Builder builder = RawMessageBatch.newBuilder();
        for (int i = 0; i < MESSAGES_IN_GROUP; i++) {
            builder.addMessages(createRawMessage(name + i, sessionAlias, factory));
        }
        return builder.build();
    }

    private static RawMessage createRawMessage(String name, String sessionAlias, CommonFactory factory) {
        return factory.getMessageFactory().createRawMessage()
                .sessionAlias(sessionAlias)
                .bytes((name).getBytes())
                .toProto(null);
    }

    private static MessageBatch createMessageBatch(String name, String sessionAlias, CommonFactory factory) {
        MessageBatch.Builder builder = MessageBatch.newBuilder();
        for (int i = 0; i < MESSAGES_IN_GROUP; i++) {
            builder.addMessages(createMessage(name + i, sessionAlias, factory));
//            builder.addMessages(createMultiLevelsMessage(name + i, sessionAlias, factory));
        }
        return builder.build();
    }

    private static Message createMessage(String name, String sessionAlias, CommonFactory factory) {
        IMessageFactory messageFactory = factory.getMessageFactory();
        return messageFactory.createParsedMessage()
                .sessionAlias(sessionAlias)
                .direction(Direction.FIRST_VALUE)
                .sequence(1L)

                .addNullField("message1_null_field")
                .addSimpleField("message1_simple_field_1", "message1_simple_field_1_value")
                .addSimpleListField("message1_simple_field_2", "message1_simple_field_2_value", "message1_simple_field_3_value")

                .addMessageField(
                        "inner_message1",
                        messageFactory.createParsedInnerMessage()
                                .addNullField("inner_message1_null_field")
                                .addSimpleField("inner_message1_simple_field_1", "inner_message1_simple_field_1_value")
                                .addSimpleListField("inner_message1_simple_field_2", "inner_message1_simple_field_2_value", "inner_message1_simple_field_3_value")
                                .addMessageField(
                                        "inner_inner_message1",
                                        messageFactory.createParsedInnerMessage().addSimpleField("inner_inner_message1_simple_field_2", "inner_inner_message1_simple_field_2_value")
                                )
                                .addMessageListField(
                                        "inner_inner_message2",
                                        messageFactory.createParsedInnerMessage().addSimpleField("inner_inner_message1_simple_field_2", "inner_inner_message1_simple_field_2_value"),
                                        messageFactory.createParsedInnerMessage().addSimpleField("inner_inner_message1_simple_field_3", "inner_inner_message1_simple_field_3_value")
                                )
                )

                .addMessageListField(
                        "inner_message_2",
                        messageFactory.createParsedInnerMessage().addNullField("inner_message_2_null_field_1"),
                        messageFactory.createParsedInnerMessage().addNullField("inner_message_2_null_field_2")
                )
                .addMessageListField(
                        "inner_message_3",
                        messageFactory.createParsedInnerMessage().addNullField("inner_message_3_null_field_1"),
                        messageFactory.createParsedInnerMessage().addNullField("inner_message_3_null_field")
                )
                .toProto(null);
    }

    private static Message createMultiLevelsMessage(String name, String sessionAlias, CommonFactory factory) {
        IMessageFactory messageFactory = factory.getMessageFactory();
        return messageFactory.createParsedMessage()
                .sessionAlias(sessionAlias)
                .addMessageField(
                        "inner_message1",
                        messageFactory.createParsedInnerMessage()
                                .addMessageField(
                                        "inner_inner_message1",
                                        messageFactory.createParsedInnerMessage()
                                                .addMessageField(
                                                        "inner_inner_inner_message1",
                                                        messageFactory.createParsedInnerMessage()
                                                                .addMessageField(
                                                                        "inner_inner_inner_inner_message1",
                                                                        messageFactory.createParsedInnerMessage().addNullField("null_field")
                                                                )
                                                )
                                )
                )
                .toProto(null);
    }

    private static MessageID createMessageId(String sessionAlias) {
        return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias).build())
                .setDirection(Direction.forNumber(ThreadLocalRandom.current().nextInt(0, 2)))
                .build();
    }

    private static EventBatch createEventBatch(String name, CommonFactory factory) throws JsonProcessingException {
        return EventBatch
                .newBuilder()
                .addEvents(createEvent(name, factory))
                .build();
    }

    private static Event createEvent(String name, CommonFactory factory) throws JsonProcessingException {
        return factory.getEventFactory().start()
                .name(name)
                .toProto(null);
    }

    private static MessageGroupBatch createMessageGroupBatch(String name, String sessionAlias, CommonFactory factory) {
        MessageGroupBatch.Builder builder = MessageGroupBatch.newBuilder();
        for (int i = 0; i < GROUPS; i++) {
            builder.addGroups(createMessageGroup(name + i, sessionAlias, factory));
        }
        return builder.build();
    }

    private static MessageGroup createMessageGroup(String name, String sessionAlias, CommonFactory factory) {
        MessageGroup.Builder builder = MessageGroup.newBuilder();
        for (int i = 0; i < MESSAGES_IN_GROUP; i++) {
            builder.addMessages(createAnyMessage(name + i, sessionAlias, factory));
        }
        return builder.build();
    }

    private static AnyMessage createAnyMessage(String name, String sessionAlias, CommonFactory factory) {
        return AnyMessage
                .newBuilder()
                .setMessage(createMessage(name, sessionAlias, factory))
                .build();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

//        factory.getMessageRouterRawBatch().send(createRawMessageGroupBatch(1, "test1"));
//        factory.getMessageRouterParsedBatch().send(createMessageGroupBatch(1, "test1"));
//        factory.getEventBatchRouter().send(createEventBatch(1));
//        factory.getMessageRouterMessageGroupBatch().send(createMessageGroupBatch(1, "test1"), "group-batch");

//    private static MessageGroupBatch createRawMessageGroupBatch(int i, String sessionAlias) {
//        return MessageGroupBatch
//                .newBuilder()
//                .addGroups(createRawMessageGroup(i, sessionAlias))
//                .build();
//    }

//    private static MessageGroup createRawMessageGroup(int i, String sessionAlias) {
//        return MessageGroup.newBuilder()
//                .addMessages(createAnyRawMessage(i, sessionAlias))
//                .build();
//    }

//    private static AnyMessage createAnyRawMessage(int i, String sessionAlias) {
//        return AnyMessage
//                .newBuilder()
//                .setRawMessage(createRawMessage(i, sessionAlias))
//                .build();
//    }

//    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        try (
//                Connection connection = factory.newConnection();
//                Channel channel = connection.createChannel()
//        ) {
//            channel.queueDeclare("hello", false, false, false, null);
//            String message = "Hello World!";
//
//            for (int i = 0; i < 100;i++) {
//                channel.basicPublish("", "hello", null, message.getBytes());
//                Thread.sleep(100L);
//            }
//        }
//    }
}
