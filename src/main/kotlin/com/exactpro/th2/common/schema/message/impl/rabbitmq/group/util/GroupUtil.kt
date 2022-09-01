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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.group.util

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch

/**
 * Transfers messages that match the [filterFunction] from current group to new group in [batchBuilder].
 *
 * If all messages are filtered no group will be added.
 *
 * If all messages match the filter the current group will be added into [batchBuilder] as is
 */
internal fun MessageGroup.filterMessagesToNewGroup(
    batchBuilder: MessageGroupBatch.Builder,
    filterFunction: (AnyMessage) -> Boolean,
): List<AnyMessage> {
    val matchCount = messagesList.count { filterFunction(it) }
    if (messagesCount == matchCount) {
        batchBuilder.addGroups(this)
        return emptyList()
    }
    if (matchCount == 0) {
        return messagesList
    }
    val newGroup = batchBuilder.addGroupsBuilder()
    val drop = ArrayList<AnyMessage>(messagesCount - matchCount)
    messagesList.forEach {
        if (filterFunction(it)) {
            newGroup.addMessages(it)
        } else {
            drop += it
        }
    }
    return drop
}