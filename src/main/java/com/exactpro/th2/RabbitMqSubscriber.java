package com.exactpro.th2;

import com.exactpro.th2.infra.grpc.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQHost;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPass;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPort;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQUser;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQVhost;

public class RabbitMqSubscriber implements Closeable {
    private static final int CLOSE_TIMEOUT = 1_000;
    private final String exchangeName;
    private final String[] routes;
    private final DeliverCallback deliverCallback;
    private final CancelCallback cancelCallback;
    private Connection connection;
    private Channel channel;

    public RabbitMqSubscriber(String exchangeName, String... routes) {
        this(exchangeName, RabbitMqSubscriber::handleReceivedMessage, null, routes);
    }

    public RabbitMqSubscriber(String exchangeName,
                              DeliverCallback deliverCallback,
                              CancelCallback cancelCallback,
                              String... routes) {
        this.exchangeName = Objects.requireNonNull(exchangeName, "exchange name is null");
        this.deliverCallback = deliverCallback;
        this.cancelCallback = cancelCallback;
        this.routes = Objects.requireNonNull(routes, "queueNames is null");
    }

    public void startListening() throws IOException, TimeoutException {
        startListening(getEnvRabbitMQHost(), getEnvRabbitMQVhost(), getEnvRabbitMQPort(), getEnvRabbitMQUser(), getEnvRabbitMQPass(), null);
    }

    public void startListening(String host, String vHost, int port, String username, String password) throws IOException, TimeoutException {
        this.startListening(host, vHost, port, username, password, null);
    }

    public void startListening(String host, String vHost, int port, String username, String password, @Nullable String subscriberName) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        if (StringUtils.isNotEmpty(vHost)) {
            factory.setVirtualHost(vHost);
        }
        factory.setPort(port);
        if (StringUtils.isNotEmpty(username)) {
            factory.setUsername(username);
        }
        if (StringUtils.isNotEmpty(password)) {
            factory.setPassword(password);
        }
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(exchangeName, "direct");
        subscribeToRoutes(exchangeName, subscriberName, routes);
    }

    @Override
    public void close() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close(CLOSE_TIMEOUT);
        }
    }

    private static void handleReceivedMessage(String consumerTag, Delivery delivery) {
        try {
            Message message = Message.parseFrom(delivery.getBody());
            if (message.getMetadata().getMessageType().equals("Heartbeat")) {
                System.out.println("HB received " + delivery.getEnvelope().getRoutingKey());
            } else {
                System.out.println("Received '" +
                        delivery.getEnvelope().getRoutingKey() + "' : '" + message.getMetadata().getMessageType() + "'");
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private void subscribeToRoutes(String exchangeName, String subscriberName, String[] routes) throws IOException {
        for (String route : routes) {
            DeclareOk declareResult = subscriberName == null ? channel.queueDeclare() : channel.queueDeclare(subscriberName + "." + System.currentTimeMillis(), false, true, true, Collections.emptyMap());
            String queue = declareResult.getQueue();
            channel.queueBind(queue, exchangeName, route);
            channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
                System.err.println("consuming cancelled for " + consumerTag);
            });
            System.out.println(String.format("Start listening '%s':'%s'", exchangeName, route));
        }
    }

    public static void main(String[] argv) throws Exception {
        RabbitMqSubscriber subscriber = new RabbitMqSubscriber("demo_exchange", "fix_client_in", "fix_client_out");
        subscriber.startListening(getEnvRabbitMQHost(), getEnvRabbitMQVhost(), getEnvRabbitMQPort(), getEnvRabbitMQUser(), getEnvRabbitMQPass(), "test_rabbit_mq_subscriber");
    }
}
