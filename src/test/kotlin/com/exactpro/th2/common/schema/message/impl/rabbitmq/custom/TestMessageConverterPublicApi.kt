/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.custom

import com.exactpro.th2.common.schema.message.impl.rabbitmq.custom.MessageConverter.Companion.create
import org.junit.jupiter.api.Test

class TestMessageConverterPublicApi {

    /**
     * Checks that the public API for MessageConverter creation is backward compatible.
     *
     * NOTE: each change of that test should be considered as a potential breaking change
     * and should be reviewed with more attention
     */
    @Test
    fun `public api is compatible`() {
        create({ it.toByteArray() }, { String(it) }, { "Debug string" })
        create({ it.toByteArray() }, { String(it) }, { "Trace string" }, { "Debug string" })
        create({ it.toByteArray() }, { String(it) }, { "Trace string" }, { "Debug string" }, { 1 })
        create({ it.toByteArray() }, { String(it) }, { "Trace string" }, { "Debug string" }, { 1 }, { arrayOf("label") })
    }
}