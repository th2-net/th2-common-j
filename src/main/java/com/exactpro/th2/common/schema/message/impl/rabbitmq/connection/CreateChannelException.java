package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

public class CreateChannelException extends RuntimeException {

    public CreateChannelException() {
        super();
    }

    public CreateChannelException(String message) {
        super(message);
    }

    public CreateChannelException(String message, Throwable cause) {
        super(message, cause);
    }
}
