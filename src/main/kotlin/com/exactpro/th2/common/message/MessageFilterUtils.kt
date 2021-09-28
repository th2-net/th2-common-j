/*
 *  Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.message

import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.event.bean.IColumn
import com.exactpro.th2.common.event.bean.TreeTable
import com.exactpro.th2.common.event.bean.TreeTableEntry
import com.exactpro.th2.common.event.bean.builder.CollectionBuilder
import com.exactpro.th2.common.event.bean.builder.RowBuilder
import com.exactpro.th2.common.event.bean.builder.TreeTableBuilder
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.ListValueFilter
import com.exactpro.th2.common.value.emptyValueFilter
import com.exactpro.th2.common.value.toValueFilter
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.MetadataFilter.SimpleFilter
import com.exactpro.th2.common.grpc.MetadataFilter.SimpleFilter.FilterValueCase.VALUE
import com.exactpro.th2.common.grpc.RootComparisonSettings
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.SimpleList
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.grpc.ValueFilter.KindCase.SIMPLE_FILTER
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty

@Deprecated(
        message = "The message type from MessageFilter will be removed in the future",
        replaceWith = ReplaceWith(
                expression = "rootMessageFilter(messageType)"
        ),
        level = DeprecationLevel.WARNING
)
fun messageFilter(messageType: String): MessageFilter.Builder = MessageFilter.newBuilder().setMessageType(messageType)
fun messageFilter(): MessageFilter.Builder = MessageFilter.newBuilder()
fun rootMessageFilter(messageType: String): RootMessageFilter.Builder = RootMessageFilter.newBuilder().setMessageType(messageType)

fun MessageFilter.getField(key: String): ValueFilter? = getFieldsOrDefault(key, null)
fun MessageFilter.Builder.getField(key: String): ValueFilter? = getFieldsOrDefault(key, null)

fun MessageFilter.Builder.addField(key: String, value: Any?): MessageFilter.Builder = apply { putFields(key, value?.toValueFilter() ?: emptyValueFilter()) }

/**
 * It accepts vararg with even size and splits it into pairs where the first value of a pair is used as a key while the second is used as a value
 */
fun MessageFilter.Builder.addFields(vararg fields: Any?): MessageFilter.Builder = apply {
    for (i in fields.indices step 2) {
        addField(fields[i] as String, fields[i + 1])
    }
}

fun MessageFilter.Builder.addFields(fields: Map<String, Any?>?): MessageFilter.Builder = apply { fields?.forEach { addField(it.key, it.value) } }

fun MessageFilter.Builder.copyField(message: MessageFilter.Builder, key: String): MessageFilter.Builder = apply { putFields(key, message.getField(key) ?: emptyValueFilter()) }
fun MessageFilter.Builder.copyField(message: MessageFilter.Builder, vararg key: String): MessageFilter.Builder = apply { key.forEach { putFields(it, message.getField(it) ?: emptyValueFilter()) } }
fun MessageFilter.Builder.copyField(message: MessageFilter, vararg key: String): MessageFilter.Builder = apply { key.forEach { putFields(it, message.getField(it) ?: emptyValueFilter()) } }

fun MessageFilter.copy(): MessageFilter.Builder = MessageFilter.newBuilder().putAllFields(fieldsMap)

fun MessageFilter.Builder.copy(): MessageFilter.Builder = MessageFilter.newBuilder().putAllFields(fieldsMap)

fun RootMessageFilter.toTreeTable(): TreeTable = TreeTableBuilder().apply {
    row("message-type", RowBuilder()
        .column(MessageTypeColumn(messageType))
        .build())
    row("message-filter", messageFilter.toTreeTableEntry())
    row("metadata-filter", metadataFilter.toTreeTableEntry())
    row("comparison-settings", comparisonSettings.toTreeTableEntry())
}.build()

fun RootMessageFilter.toReadableBodyCollection(additionalMetadata: Map<String, String>? = null): Collection<IBodyData> =
    listOf(
        TreeTableBuilder("Metadata").apply {
            row("message-type", RowBuilder().column(MetadataField(messageType)).build())
            additionalMetadata?.forEach {
                row(it.key, RowBuilder().column(MetadataField(it.value)).build())
            }
        }.build(),
        TreeTableBuilder("Filter").apply {
            row("message-filter", messageFilter.toTreeTableEntry())
            row("metadata-filter", metadataFilter.toTreeTableEntry())
        }.build(),
        TreeTableBuilder("Settings").apply {
            row("comparison-settings", comparisonSettings.toTreeTableEntry())
        }.build()
    )

fun MessageFilter.toTreeTable(): TreeTable = TreeTableBuilder().apply {
    for ((key, value) in fieldsMap) {
        row(key, value.toTreeTableEntry())
    }
}.build()

private fun MessageFilter.toTreeTableEntry(): TreeTableEntry = CollectionBuilder().apply {
    for ((key, valueFilter) in fieldsMap) {
        row(key, valueFilter.toTreeTableEntry())
    }
}.build()

private fun MetadataFilter.toTreeTableEntry(): TreeTableEntry = CollectionBuilder().apply {
    for ((key, simpleFilter) in propertyFiltersMap) {
        row(key, simpleFilter.toTreeTableEntry())
    }
}.build()

private fun RootComparisonSettings.toTreeTableEntry(): TreeTableEntry = CollectionBuilder().apply {
    row("ignore-fields", CollectionBuilder().apply {
        ignoreFieldsList.forEachIndexed { index, nestedValue ->  
            val nestedName = index.toString()
            row(nestedName, RowBuilder()
                .column(IgnoreFieldColumn(nestedValue))
                .build())
        }
    }.build())
}.build()

private fun ListValueFilter.toTreeTableEntry(): TreeTableEntry = CollectionBuilder().apply {
    valuesList.forEachIndexed { index, nestedValue ->
        val nestedName = index.toString()
        row(nestedName, nestedValue.toTreeTableEntry())
    }
}.build()

private fun SimpleFilter.toTreeTableEntry(): TreeTableEntry = when {
    hasSimpleList() -> simpleList.toTreeTableEntry(operation, key)
    filterValueCase == VALUE -> RowBuilder()
        .column(MessageFilterTableColumn(value, operation.toString(), key))
        .build()
    else -> error("Unsupported simple filter value: $filterValueCase")
}

private fun ValueFilter.toTreeTableEntry(): TreeTableEntry = when {
    hasMessageFilter() -> messageFilter.toTreeTableEntry()
    hasListFilter() -> listFilter.toTreeTableEntry()
    hasSimpleList() -> simpleList.toTreeTableEntry(operation, key)
    kindCase == SIMPLE_FILTER -> RowBuilder()
        .column(MessageFilterTableColumn(simpleFilter, operation.toString(), key))
        .build()
    else -> error("Unsupported ValueFilter value: $kindCase")
}

private fun SimpleList.toTreeTableEntry(operation: FilterOperation, key: Boolean): TreeTableEntry {
    return RowBuilder()
        .column(MessageFilterTableColumn(simpleValuesList.joinToString(prefix = "[", postfix = "]"), operation.toString(), key))
        .build()
}

private data class MessageFilterTableColumn(
    @JsonIgnore val value: String,
    @JsonIgnore val operation: String,
    val key: Boolean
) : IColumn {
    @get:JsonProperty(index = 0)
    val expected: String
        get() = if (value.isEmpty()) operation else "$operation '${value}'"
}

private data class MetadataField(
    @get:JsonProperty(value = "Expected field value") val metadataValue: String
) : IColumn

private data class MessageTypeColumn(val type: String) : IColumn
private data class IgnoreFieldColumn(val name: String) : IColumn
