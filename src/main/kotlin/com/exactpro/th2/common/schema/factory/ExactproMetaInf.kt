/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import mu.KotlinLogging
import java.io.IOException
import java.net.URL
import java.nio.file.Path
import java.util.Properties
import java.util.jar.Attributes
import java.util.jar.JarFile
import java.util.jar.Manifest

internal class ExactproMetaInf(
    url: URL,
    private val title: String,
    private val version: String
) {
    private val jarPath: Path = url.toPath().parent.parent

    private var gitEnriched = false
    private var gitHash = ""
    private var gitBranch = ""
    private var gitRemoteUrl = ""
    private var gitClosestTag = ""

    fun enrich(gitProperty: URL) {
        try {
            gitProperty.openStream().use { inputStream ->
                val properties = Properties()
                properties.load(inputStream)
                gitHash = properties.getProperty(GIT_HASH_PROPERTY)
                gitBranch = properties.getProperty(GIT_BRANCH_PROPERTY)
                gitRemoteUrl = properties.getProperty(GIT_REMOTE_URL_PROPERTY)
                gitClosestTag = properties.getProperty(GIT_CLOSEST_TAG_PROPERTY)
                gitEnriched = true
            }
        } catch (e: IOException) {
            K_LOGGER.warn(e) { "Git properties '$gitProperty' loading failure" }
        }
    }

    override fun toString(): String {
        return "Manifest title: $title, version: $version , git { ${
            if (gitEnriched) {
                "hash: $gitHash, branch: $gitBranch, repository: $gitRemoteUrl, closest tag: $gitClosestTag"
            } else {
                "'${jarPath.fileName}' jar doesn't contain '$GIT_PROPERTIES_FILE' resource, please use '$GRADLE_GIT_PROPERTIES_PLUGIN' plugin"
            }
        } }"
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private const val EXACTPRO_IMPLEMENTATION_VENDOR = "Exactpro Systems LLC"
        private const val GRADLE_GIT_PROPERTIES_PLUGIN = "com.gorylenko.gradle-git-properties"
        private const val GIT_PROPERTIES_FILE = "git.properties"
        private const val GIT_HASH_PROPERTY = "git.commit.id"
        private const val GIT_BRANCH_PROPERTY = "git.branch"
        private const val GIT_REMOTE_URL_PROPERTY = "git.remote.origin.url"
        private const val GIT_CLOSEST_TAG_PROPERTY = "git.closest.tag.name"

        @JvmStatic
        fun logging() {
            if (K_LOGGER.isInfoEnabled) {
                try {
                    val map = Thread.currentThread().contextClassLoader
                        .getResources(JarFile.MANIFEST_NAME).asSequence()
                        .mapNotNull(::create)
                        .map { metaInf -> metaInf.jarPath to metaInf }
                        .toMap()

                    Thread.currentThread().contextClassLoader
                        .getResources(GIT_PROPERTIES_FILE).asSequence()
                        .forEach { url -> map[url.toPath().parent]?.enrich(url) }

                    map.values.forEach { metaInf -> K_LOGGER.info { "$metaInf" } }
                } catch (e: Exception) {
                    K_LOGGER.warn(e) { "Manifest searching failure" }
                }
            }
        }

        private fun create(manifestUrl: URL): ExactproMetaInf? {
            try {
                manifestUrl.openStream().use { inputStream ->
                    val attributes = Manifest(inputStream).mainAttributes
                    return if (EXACTPRO_IMPLEMENTATION_VENDOR != attributes.getValue(Attributes.Name.IMPLEMENTATION_VENDOR)) {
                        null
                    } else ExactproMetaInf(
                        manifestUrl,
                        attributes.getValue(Attributes.Name.IMPLEMENTATION_TITLE),
                        attributes.getValue(Attributes.Name.IMPLEMENTATION_VERSION)
                    )
                }
            } catch (e: Exception) {
                K_LOGGER.warn(e) { "Manifest '$manifestUrl' loading failure" }
                return null
            }
        }

        internal fun URL.toPath(): Path = when (protocol) {
            "jar" -> URL(file).toPath() // this code is added to provide windows compatibility
            "file" -> Path.of(path)
            else -> error("The '$protocol' protocol of '$this' URL can't be handled")
        }
    }
}
