/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.common.schema.message;

public interface ManualConfirmationListener<T> extends ConfirmationMessageListener<T> {
    @Deprecated
    default void handle(String consumerTag, T message, ManualAckDeliveryCallback.Confirmation confirmation) throws Exception {
        handle(new DeliveryMetadata(consumerTag, false), message, confirmation);
    }

    void handle(DeliveryMetadata deliveryMetadata, T message, ManualAckDeliveryCallback.Confirmation confirmation) throws Exception;

    static <T> ManualConfirmationListener<T> wrap(MessageListener<T> listener) {
        return new ManualDelegateListener<>(listener);
    }
}

class ManualDelegateListener<T> implements ManualConfirmationListener<T> {
    private final MessageListener<T> delegate;
    ManualDelegateListener(MessageListener<T> listener) {
        delegate = listener;
    }

    @Override
    public void handle(DeliveryMetadata deliveryMetadata, T message, ManualAckDeliveryCallback.Confirmation confirmation) throws Exception {
        delegate.handle(deliveryMetadata, message);
    }

    @Override
    public void onClose() {
        delegate.onClose();
    }
}