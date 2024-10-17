/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.factory

import com.exactpro.th2.common.schema.factory.ExactproMetaInf.Companion.toPath
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.net.URL
import java.nio.file.Path
import kotlin.test.assertEquals

class ExactproMetaInfTest {

    @ParameterizedTest
    @CsvSource(
        """jar:file:/C:/Users/test!user/common-0.0.0.jar!/META-INF/MANIFEST.MF,/C:/Users/test!user/common-0.0.0.jar!/META-INF/MANIFEST.MF""",
        """jar:file:/home/user/common-0.0.0-dev.jar!/META-INF/MANIFEST.MF,/home/user/common-0.0.0-dev.jar!/META-INF/MANIFEST.MF"""
    )
    fun `convert url to path test`(url: String, path: String) {
        assertEquals(Path.of(path), URL(url).toPath())
    }
}