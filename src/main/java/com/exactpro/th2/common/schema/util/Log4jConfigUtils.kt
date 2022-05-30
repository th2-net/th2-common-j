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
        secondVersionPriority: Boolean = true
    ) {
        val configPathList: Pair<Path, Int>? = pathList
            .flatMap {
                listOf(
                    Pair(Path.of(it, firstVersionFileName), 1),
                    Pair(Path.of(it, secondVersionFileName), 2)
                )
            }
            .filter { Files.exists(it.first) }
            .run { if (secondVersionPriority) sortedByDescending { it.second } else this }
            .getOrNull(0)

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
            PropertyConfigurator.configure(path.toUri().toURL())
            LOGGER.info("Logger configuration from {} file is applied", path)
        } catch (e: MalformedURLException) {
            e.printStackTrace()
        }
    }

    private fun configureSecondLog4j(path: Path) {
        val context = LogManager.getContext(false) as LoggerContext
        context.configLocation = path.toUri()
        LOGGER.info("Logger configuration from {} file is applied", path)
    }

}