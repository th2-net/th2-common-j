package com.exactpro.th2.common.schema.message.impl.rabbitmq.connection;

import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ResendMessageConfiguration;
import com.rabbitmq.client.AMQP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class SendData {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendData.class);

    private final String exchange;
    private final String routingKey;
    private final AMQP.BasicProperties props;
    private final byte[] body;
    private final Function<ResendMessageConfiguration, Long> delayFunction;
    private final Consumer<ResendMessageConfiguration> ackHandler;

    public SendData(String exchange,
                    String routingKey,
                    AMQP.BasicProperties props,
                    byte[] body,
                    Function<ResendMessageConfiguration, Long> delayFunction,
                    Consumer<ResendMessageConfiguration> ackHandler) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.props = props;
        this.body = body;
        this.delayFunction = delayFunction;
        this.ackHandler = ackHandler;
    }

    public SendData(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) {
        this(exchange, routingKey, props, body, null, null);
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public AMQP.BasicProperties getProps() {
        return props;
    }

    public byte[] getBody() {
        return body;
    }

    public void send(Resender resender) {
        resender.sendAsync(this);
    }

    public void success(ResendMessageConfiguration configuration) {
        if (ackHandler != null) {
            try {
                ackHandler.accept(configuration);
            } catch (Exception e) {
                LOGGER.warn("Can not execute ack handler for exchange '{}', routing key '{}'", exchange, routingKey, e);
            }
        }
    }

    public void resend(ScheduledExecutorService scheduler, ResendMessageConfiguration configuration, Resender resender) {
        if (delayFunction != null) {
            Long delay = 0L;
            try {
                delay = delayFunction.apply(configuration);
            } catch (Exception e) {
                LOGGER.warn("Can not get delay for message to exchange '{}', routing key '{}'", exchange, routingKey, e);
            }

            if (delay != null && delay > -1) {
                if (delay > 0) {
                    LOGGER.trace("Wait for resend message to exchange '{}', routing key '{}' milliseconds = {}", exchange, routingKey, delay);
                    scheduler.schedule(() -> this.send(resender), delay, TimeUnit.MILLISECONDS);
                } else {
                    LOGGER.trace("Not wait for send message to exchange '{}', routing key '{}'", exchange, routingKey);
                    this.send(resender);
                }
            }
        }
    }
}
