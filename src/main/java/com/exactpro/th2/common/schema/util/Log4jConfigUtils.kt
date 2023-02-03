/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.common.schema.util

import org.apache.logging.log4j.core.LoggerContext
import org.slf4j.LoggerFactory
import java.net.MalformedURLException
import java.nio.file.Files
import java.nio.file.Path
import org.apache.logging.log4j.core.LoggerContext
import org.slf4j.LoggerFactory

class Log4jConfigUtils {

    fun configure(
        pathList: List<String>,
        fileName: String,
    ) {
        pathList.asSequence()
            .map { Path.of(it, fileName) }
            .filter(Files::exists)
            .firstOrNull()
            ?.let { path ->
                try {
                    LOGGER.info("Trying to apply logger config from {}. Expecting log4j syntax", path)
                    val loggerContext = LoggerContext.getContext(false)
                    loggerContext.configLocation = path.toUri()
                    loggerContext.reconfigure()
                    LOGGER.info("Logger configuration from {} file is applied", path)
                } catch (e: MalformedURLException) {
                    LOGGER.error(e.message, e)
                }
            }
            ?: run {
                LOGGER.info(
                    "Neither of {} paths contains config file {}. Use default configuration",
                    pathList,
                    fileName
                )
            }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(Log4jConfigUtils::class.java)
    }
}