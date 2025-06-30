/*
 *  Copyright 2024 Exactpro (Exactpro Systems Limited)
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.schema.configuration.impl

import com.exactpro.th2.common.schema.configuration.IDictionaryProvider
import com.exactpro.th2.common.schema.configuration.impl.DictionaryKind.ALIAS
import com.exactpro.th2.common.schema.configuration.impl.DictionaryKind.OLD
import com.exactpro.th2.common.schema.configuration.impl.DictionaryKind.TYPE
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.util.ArchiveUtils
import org.apache.commons.io.FilenameUtils
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Locale
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile
import kotlin.streams.asSequence

class DictionaryProvider private constructor(
    baseDir: Path,
    paths: Map<DictionaryKind, Path> = emptyMap()
) : IDictionaryProvider {

    private val directoryPaths = DictionaryKind.createMapping(baseDir)
        .plus(paths.mapValues { (_, value) -> value.toAbsolutePath() })

    private val dictionaryOldPath: Path
        get() = requireNotNull(directoryPaths[OLD]) {
            "$OLD dictionary kind isn't present in $directoryPaths"
        }
    private val dictionaryTypePath: Path
        get() = requireNotNull(directoryPaths[TYPE]) {
            "$TYPE dictionary kind isn't present in $directoryPaths"
        }

    private val dictionaryAliasPath: Path
        get() = requireNotNull(directoryPaths[ALIAS]) {
            "$ALIAS dictionary kind isn't present in $directoryPaths"
        }
    private val directories = directoryPaths.values

    override fun aliases(): Set<String> {
        try {
            if (!dictionaryAliasPath.isDirectory()) {
                return emptySet()
            }

            val fileList = Files.walk(dictionaryAliasPath, 1).asSequence()
                .filter(Path::isRegularFile)
                .toList()
            val aliasSet: MutableSet<String> = mutableSetOf()
            val duplicates: MutableMap<String, MutableSet<String>> = mutableMapOf()
            for (path in fileList) {
                val alias = FilenameUtils.removeExtension(path.fileName.toString())
                    .lowercase(Locale.getDefault())
                if (!aliasSet.add(alias)) {
                    duplicates.getOrPut(alias, ::mutableSetOf).add(path.fileName.toString())
                }
            }
            check(duplicates.isEmpty()) {
                "Dictionary directory contains files with the same name in different cases, files: $duplicates, path: $dictionaryAliasPath"
            }
            return aliasSet
        } catch (e: IOException) {
            throw IllegalStateException(
                "Can not get dictionaries aliases from path: ${dictionaryAliasPath.toAbsolutePath()}",
                e
            )
        }
    }

    override fun load(alias: String): InputStream {
        try {
            require(alias.isNotBlank()) {
                "Dictionary is blank"
            }

            check(dictionaryAliasPath.isDirectory()) {
                "Dictionary dir doesn't exist or isn't directory, path ${dictionaryAliasPath.toAbsolutePath()}"
            }

            val files = searchInAliasDir(alias)
            val file = single(listOf(dictionaryAliasPath), files, alias)

            return open(file)
        } catch (e: IOException) {
            throw IllegalStateException(
                "Can not load dictionary by '$alias' alias from path: ${dictionaryAliasPath.toAbsolutePath()}",
                e
            )
        }
    }

    @Deprecated("Load dictionary by type is deprecated, please use load by alias")
    override fun load(type: DictionaryType): InputStream {
        try {
            var files = searchInAliasDir(type.name)

            if (files.isEmpty()) {
                files = searchInTypeDir(type)
            }

            if (files.isEmpty()) {
                files = searchInOldDir(type.name)
            }

            val file = single(directories, files, type.name)
            return open(file)
        } catch (e: IOException) {
            throw IllegalStateException("Can not load dictionary by '$type' type from paths: $directories", e)
        }
    }

    @Deprecated("Load single dictionary is deprecated, please use load by alias")
    override fun load(): InputStream {
        val dirs = listOf(dictionaryAliasPath, dictionaryTypePath)
        try {
            var files: List<Path> = if (dictionaryAliasPath.isDirectory()) {
                Files.walk(dictionaryAliasPath, 1).asSequence()
                    .filter(Path::isRegularFile)
                    .toList()
            } else {
                emptyList()
            }

            if (files.isEmpty()) {
                if (dictionaryTypePath.isDirectory()) {
                    files = Files.walk(dictionaryTypePath, 1).asSequence()
                        .filter(Path::isDirectory)
                        .flatMap { dir ->
                            Files.walk(dir, 1).asSequence()
                                .filter(Path::isRegularFile)
                        }.toList()
                }
            }

            check(files.isNotEmpty()) {
                "No dictionary at path(s): $dirs"
            }
            check(files.size == 1) {
                "Found several dictionaries at paths: $dirs"
            }
            val file = files.single()
            return open(file)
        } catch (e: IOException) {
            throw IllegalStateException("Can not read dictionary from from paths: $directories", e)
        }
    }

    private fun open(file: Path) = ByteArrayInputStream(
        ArchiveUtils.getGzipBase64StringDecoder().decode(Files.readString(file))
    )

    private fun single(dirs: Collection<Path>, files: List<Path>, alias: String): Path {
        check(files.isNotEmpty()) {
            "No dictionary was found by '$alias' name at path(s): $dirs"
        }
        check(files.size == 1) {
            "Found several dictionaries by '$alias' name at path(s): $dirs"
        }
        return files.single()
    }

    private fun searchInOldDir(name: String): List<Path> {
        if (dictionaryOldPath.isDirectory()) {
            return Files.walk(dictionaryOldPath, 1).asSequence()
                .filter(Path::isRegularFile)
                .filter { file -> file.fileName.toString().contains(name) }
                .toList()
        }
        return emptyList()
    }

    private fun searchInTypeDir(type: DictionaryType): List<Path> {
        val path = type.getDictionary(dictionaryTypePath)
        if (path.isDirectory()) {
            return Files.walk(path, 1).asSequence()
                .filter(Path::isRegularFile)
                .toList()
        }
        return emptyList()
    }

    private fun searchInAliasDir(alias: String): List<Path> {
        if (dictionaryAliasPath.isDirectory()) {
            return Files.walk(dictionaryAliasPath, 1).asSequence()
                .filter(Path::isRegularFile)
                .filter { file -> alias.equals(toAlias(file), true) }
                .toList()
        }
        return emptyList()
    }

    companion object {
        private fun toAlias(path: Path) = FilenameUtils.removeExtension(path.fileName.toString())

        @JvmStatic
        @JvmOverloads
        fun create(
            baseDir: Path,
            paths: Map<DictionaryKind, Path> = emptyMap()
        ): DictionaryProvider = DictionaryProvider(baseDir, paths)
    }
}

enum class DictionaryKind(
    val directoryName: String
) {
    OLD(""),
    TYPE("dictionary"),
    ALIAS("dictionaries");

    companion object {
        fun createMapping(baseDir: Path): Map<DictionaryKind, Path> {
            return buildMap {
                DictionaryKind.values().forEach {
                    put(it, baseDir.resolve(it.directoryName).toAbsolutePath())
                }
            }
        }
    }
}