package com.exactpro.th2.common.schema.grpc.service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRetryConfiguration;
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration;
import com.google.protobuf.Message;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;

public abstract class AbstractGrpcService<S extends AbstractStub> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass() + "@" + this.hashCode());
    private final GrpcRetryConfiguration retryConfiguration;
    private final GrpcServiceConfiguration serviceConfiguration;
    private final Map<String, S> stubs = new ConcurrentHashMap<>();

    public AbstractGrpcService(@NotNull GrpcRetryConfiguration retryConfiguration, @NotNull GrpcServiceConfiguration serviceConfiguration) {
        this.retryConfiguration = Objects.requireNonNull(retryConfiguration, "Retry configuration can not be null");
        this.serviceConfiguration = Objects.requireNonNull(serviceConfiguration, "Service configuration can not be null");
    }

    protected <T> T createBlockingRequest(Supplier<T> method) {

        RuntimeException exception = new RuntimeException("Can not execute GRPC blocking request");

        for (int i = 0; i < retryConfiguration.getMaxRetriesAttempts(); i++) {
            try {
                return method.get();
            } catch (StatusRuntimeException e) {
                exception.addSuppressed(e);
                logger.warn("Can not send GRPC blocking request. Retrying. Current attempt = {}", i + 1, e);
            } catch (Exception e) {
                exception.addSuppressed(e);
                throw exception;
            }

            try {
                Thread.sleep(retryConfiguration.getRetryDelay(i));
            } catch (InterruptedException e) {
                exception.addSuppressed(e);
                throw exception;
            }
        }

        throw exception;
    }

    protected <T> void createAsyncRequest(StreamObserver<T> observer,  Consumer<StreamObserver<T>> method) {
        method.accept(new RetryStreamObserver<>(observer, method));
    }

    protected abstract S createStub(Channel channel);

    protected S getStub(Message message) {
        String endpointName = serviceConfiguration.getStrategy().getEndpoint(message);
        return stubs.computeIfAbsent(endpointName, (key) -> {
            GrpcEndpointConfiguration endpoint = serviceConfiguration.getEndpoints().get(key);

            if (Objects.isNull(endpoint)) {
                throw new IllegalStateException("No endpoint in configuration " +
                        "that matching the provided alias: " + key);
            }

            return createStub(ManagedChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()).usePlaintext().build());
        });
    }

    private class RetryStreamObserver<T> implements StreamObserver<T> {

        private final StreamObserver<T> delegate;
        private final Consumer<StreamObserver<T>> method;
        private final AtomicInteger currentAttempt = new AtomicInteger(0);

        public RetryStreamObserver(StreamObserver<T> delegate, Consumer<StreamObserver<T>> method) {
            this.delegate = delegate;
            this.method = method;
        }

        @Override
        public void onNext(T value) {
            delegate.onNext(value);
        }

        @Override
        public void onError(Throwable t) {
            if (currentAttempt.get() < retryConfiguration.getMaxRetriesAttempts() && t instanceof StatusRuntimeException) {

                logger.warn("Can not send GRPC async request. Retrying. Current attempt = {}", currentAttempt.get() + 1, t);

                try {
                    Thread.sleep(retryConfiguration.getRetryDelay(currentAttempt.getAndIncrement()));

                    method.accept(this);
                } catch (InterruptedException e) {
                    delegate.onError(t);
                }
            } else {
                delegate.onError(t);
            }
        }

        @Override
        public void onCompleted() {
            delegate.onCompleted();
        }
    }

}
