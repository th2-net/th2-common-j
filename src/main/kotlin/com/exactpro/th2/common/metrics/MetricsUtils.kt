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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.getSessionAliasAndDirection
import com.exactpro.th2.common.message.sequence
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

fun incrementTotalMetrics(
    batch: MessageGroupBatch,
    th2Pin: String,
    messageCounter: Counter,
    groupCounter: Counter,
    gauge: Gauge
) {
    val groupsBySessionAliasAndDirection = mutableMapOf<SessionAliasAndDirection, InternalCounter>()
    batch.groupsList.forEach { group ->
        if (group.messagesList.isNotEmpty()) {
            val aliasAndDirectionArray = getSessionAliasAndDirection(group.messagesList[0])
            incrementMetricByMessages(
                group.messagesList,
                th2Pin,
                messageCounter,
                aliasAndDirectionArray[0],
                aliasAndDirectionArray[1]
            )

            val aliasAndDirection = SessionAliasAndDirection(aliasAndDirectionArray[0], aliasAndDirectionArray[1])
            groupsBySessionAliasAndDirection
                .computeIfAbsent(aliasAndDirection) { InternalCounter() }
                .inc()

            gauge
                .labels(th2Pin, *aliasAndDirection.labels)
                .set(group.messagesList[0].sequence.toDouble())
        }
    }
    groupsBySessionAliasAndDirection.forEach {
        groupCounter
            .labels(th2Pin, *it.key.labels)
            .inc(it.value.number.toDouble())
    }
}

fun incrementDroppedMetrics(
    messages: List<AnyMessage>,
    th2Pin: String,
    messageCounter: Counter,
    groupCounter: Counter
) {
    if (messages.isNotEmpty()) {
        val aliasAndDirectionArray = getSessionAliasAndDirection(messages[0])
        incrementMetricByMessages(
            messages,
            th2Pin,
            messageCounter,
            aliasAndDirectionArray[0],
            aliasAndDirectionArray[1]
        )
        groupCounter
            .labels(th2Pin, aliasAndDirectionArray[0], aliasAndDirectionArray[1])
            .inc()
    }
}

private fun incrementMetricByMessages(
    messages: List<AnyMessage>,
    th2Pin: String,
    counter: Counter,
    sessionAlias: String,
    direction: String
) {
    messages
        .groupingBy { it.kindCase.name }
        .eachCount()
        .forEach {
            counter
                .labels(th2Pin, sessionAlias, direction, it.key)
                .inc(it.value.toDouble())
        }
}

private data class SessionAliasAndDirection(val sessionAlias: String, val direction: String) {
    val labels = arrayOf(sessionAlias, direction)
}

private class InternalCounter {
    var number: Int = 0
    fun inc() = number++
}