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

import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toJson
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class TestGroupUtilKt {
    @Test
    fun `adds original group it all match filter`() {
        val group = MessageGroup.newBuilder().apply {
            this += message("A")
            this += message("A")
            this += message("A")
        }.build()

        val batch = MessageGroupBatch.newBuilder()

        val dropped = group.filterMessagesToNewGroup(batch) { true }

        Assertions.assertTrue(dropped.isEmpty()) { "unexpected dropped messages $dropped" }
        Assertions.assertEquals(1, batch.groupsCount) { "unexpected groups: ${batch.toJson()}" }
        Assertions.assertSame(group, batch.getGroups(0)) { "unexpected group: ${batch.getGroups(0)}" }
    }

    @Test
    fun `adds only filtered messages to the group`() {
        val group = MessageGroup.newBuilder().apply {
            this += message("A")
            this += message("B")
            this += message("C")
        }.build()

        val batch = MessageGroupBatch.newBuilder()

        val dropped = group.filterMessagesToNewGroup(batch) { it.message.messageType == "A" }

        Assertions.assertEquals(group.messagesList.drop(1), dropped) { "unexpected dropped messages: ${dropped.joinToString { it.toJson() }}" }
        Assertions.assertEquals(1, batch.groupsCount) { "unexpected groups: ${batch.toJson()}" }
        Assertions.assertEquals(group.messagesList.take(1), batch.getGroups(0).messagesList) { "unexpected group: ${batch.getGroups(0)}" }
    }

    @Test
    fun `does not add group if all messages dropped`() {
        val group = MessageGroup.newBuilder().apply {
            this += message("A")
            this += message("B")
            this += message("C")
        }.build()

        val batch = MessageGroupBatch.newBuilder()

        val dropped = group.filterMessagesToNewGroup(batch) { false }

        Assertions.assertEquals(group.messagesList, dropped) { "unexpected dropped messages: ${dropped.joinToString { it.toJson() }}" }
        Assertions.assertEquals(0, batch.groupsCount) { "unexpected groups: ${batch.toJson()}" }
    }
}