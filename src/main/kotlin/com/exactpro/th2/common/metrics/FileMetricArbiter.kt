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
package com.exactpro.th2.common.metrics

import java.nio.file.Files
import java.nio.file.Path

class FileMetricArbiter(fileName: String) : AbstractMetricArbiter() {
    private val metricFile: Path = DEFAULT_PATH_TO_METRIC_FOLDER.resolve(fileName)

    init {
        Files.deleteIfExists(metricFile)
    }

    override fun metricChangedValue(value: Boolean) {
        if (value) {
            try {
                Files.createFile(metricFile)
            } catch (e: FileAlreadyExistsException) {
                // Do nothing
            } catch (e: Exception) {
                throw IllegalStateException("Can not create file = $metricFile", e)
            }
        } else {
            Files.deleteIfExists(metricFile)
        }
    }

    companion object {
        private val DEFAULT_PATH_TO_METRIC_FOLDER = Path.of("/tmp")
    }
}