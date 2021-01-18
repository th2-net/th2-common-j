/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.factory.extensions

import com.exactpro.th2.common.schema.factory.AbstractCommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.MessageConverter
import com.fasterxml.jackson.databind.ObjectMapper

inline fun <reified T : Any> AbstractCommonFactory.getCustomMessageRouter(): MessageRouter<T> = getCustomMessageRouter(T::class.java)

inline fun <reified T : Any> AbstractCommonFactory.getCustomConfiguration(customMapper: ObjectMapper? = null): T {
    return if (customMapper == null) getCustomConfiguration(T::class.java) else getCustomConfiguration(T::class.java, customMapper)
}

inline fun <reified T : Any> AbstractCommonFactory.registerCustomMessageRouter(
    converter: MessageConverter<T>,
    defaultSendAttributes: Set<String> = emptySet(),
    defaultSubscribeAttributes: Set<String> = emptySet()
) {
    registerCustomMessageRouter(T::class.java, converter, defaultSendAttributes, defaultSubscribeAttributes)
}