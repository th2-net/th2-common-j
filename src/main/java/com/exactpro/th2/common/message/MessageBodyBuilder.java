/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.message;

import java.util.Collection;
import java.util.function.Consumer;

public interface MessageBodyBuilder {
    MessageBodyBuilder putSimpleField(String name, Object value);

    MessageBodyBuilder putSimpleField(String name, Collection<Object> value);

    MessageBodyBuilder putMessage(String name, Consumer<MessageBodyBuilder> setup);

    /**
     * Builds new message and setups it via the {@code setup} method.
     * After that add the sub message to a list messages by the {@code name} field name.
     * This method creates new list container if the current level doesn't contain the {@code name} field
     * @param name is the filed name
     * @param setup is the sub message setup method
     * @return the {@link MessageBodyBuilder} instance related to the current level of message
     * @throws IllegalStateException if existed value got by the {@code name} isn't collection of messages
     */
    MessageBodyBuilder addMessage(String name, Consumer<MessageBodyBuilder> setup);

    /**
     * Builds and setups new messages for each {@code setup} method.
     * After that add the sub messages to a list messages got by the {@code name} field name.
     * This method creates new list container if the current level doesn't contain the {@code name} field
     * @param name is the filed name
     * @param setup is the sub message setup methods
     * @return the {@link MessageBodyBuilder} instance related to the current level of message
     * @throws IllegalStateException if existed value got by the {@code name} isn't collection of messages
     */
    MessageBodyBuilder addMessages(String name, Collection<Consumer<MessageBodyBuilder>> setup);

    MessageBodyBuilder putMessages(String name, Collection<Consumer<MessageBodyBuilder>> setup);
}
