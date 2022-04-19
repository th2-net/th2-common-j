/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

@file:JvmName("FieldValueChecker")
package com.exactpro.th2.common.schema.filter.strategy.impl

import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation
import org.apache.commons.io.FilenameUtils

fun FieldFilterConfiguration.checkFieldValue(value: String?) =
    when (operation) {
        FieldFilterOperation.EQUAL -> value == expectedValue
        FieldFilterOperation.NOT_EQUAL -> value != expectedValue
        FieldFilterOperation.EMPTY -> value.isNullOrEmpty()
        FieldFilterOperation.NOT_EMPTY -> !value.isNullOrEmpty()
        FieldFilterOperation.WILDCARD -> FilenameUtils.wildcardMatch(value, expectedValue)
    }