/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.monitor

import com.exactpro.th2.common.schema.message.MessageRouterMonitor
import org.slf4j.LoggerFactory

class LogMessageRouterMonitor : MessageRouterMonitor {

    override fun onInfo(msg: String, vararg args: Any?) {
        LOGGER.info(msg, args)
    }

    override fun onWarn(msg: String, vararg args: Any?) {
        LOGGER.warn(msg, args);
    }

    override fun onError(msg: String, vararg args: Any?) {
        LOGGER.error(msg, args)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(LogMessageRouterMonitor::class.java)
    }
}