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

import org.apache.log4j.PropertyConfigurator
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LoggerContext
import org.slf4j.LoggerFactory
import java.net.MalformedURLException
import java.nio.file.Files
import java.nio.file.Path

class Log4jConfigUtils {

    private val LOGGER = LoggerFactory.getLogger(Log4jConfigUtils::class.java)

    fun configure(
        pathList: List<String>,
        firstVersionFileName: String,
        secondVersionFileName: String,
    ) {
        val configPathList: Pair<Path, Int>? = pathList.asSequence()
            .flatMap {
                listOf(
                    Pair(Path.of(it, firstVersionFileName), 1),
                    Pair(Path.of(it, secondVersionFileName), 2)
                )
            }
            .filter { Files.exists(it.first) }
            .sortedByDescending { it.second  }
            .firstOrNull()

        when (configPathList?.second) {
            1 -> configureFirstLog4j(configPathList.first)
            2 -> configureSecondLog4j(configPathList.first)
            null -> {
                LOGGER.info(
                    "Neither of {} paths contains config files {}, {}. Use default configuration",
                    pathList,
                    firstVersionFileName,
                    secondVersionFileName
                )
            }
        }
    }

    private fun configureFirstLog4j(path: Path) {
        try {
            LOGGER.info("Trying to apply logger config from {}. Expecting log4j syntax", path)
            PropertyConfigurator.configure(path.toUri().toURL())
            LOGGER.info("Logger configuration from {} file is applied", path)
        } catch (e: MalformedURLException) {
            e.printStackTrace()
        }
    }

    private fun configureSecondLog4j(path: Path) {
        LOGGER.info("Trying to apply logger config from {}. Expecting log4j2 syntax", path)
        val context = LogManager.getContext(false) as LoggerContext
        context.configLocation = path.toUri()
        LOGGER.info("Logger configuration from {} file is applied", path)
    }

}