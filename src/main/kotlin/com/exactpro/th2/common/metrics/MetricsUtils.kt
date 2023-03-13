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
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.getSessionAliasAndDirection
import com.exactpro.th2.common.message.sequence
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import kotlin.math.max

@Deprecated("This old implementation for jmh test only. Please use the incrementTotalMetrics method instead")
fun incrementTotalMetricsOld(
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

data class SessionStats(
    var sequence: Long = 0L,
    var groups: Int = 0,
    var messages: Int = 0,
    var rawMessages: Int = 0,
)

//FIXME: com.exactpro.th2.common.metrics.MetricsUtilsKt.incrementTotalMetrics() 30,374 ms (12.5%)
fun incrementTotalMetrics(
    batch: MessageGroupBatch,
    th2Pin: String,
    messageCounter: Counter,
    groupCounter: Counter,
    gauge: Gauge,
) {
    val incomingStatsBySession = mutableMapOf<String, SessionStats>()
    val outgoingStatsBySession = mutableMapOf<String, SessionStats>()

    //FIXME: java.util.Collections$UnmodifiableCollection.iterator() 14,357 ms (5.9%)
    for (group in batch.groupsList) {
        val messages = group.messagesList

        if (messages.isEmpty()) continue

        val firstMessage = messages[0]
        val id = if (firstMessage.hasMessage()) firstMessage.message.metadata.id else firstMessage.rawMessage.metadata.id
        val sessionAlias = id.connectionId.sessionAlias

        val stats = when (id.direction) {
            FIRST -> incomingStatsBySession.getOrPut(sessionAlias, ::SessionStats)
            else -> outgoingStatsBySession.getOrPut(sessionAlias, ::SessionStats)
        }

        stats.groups++

        messages.forEach {
            when {
                it.hasMessage() -> stats.messages++
                else -> stats.rawMessages++
            }
        }

        stats.sequence = max(stats.sequence, id.sequence)
    }

    incomingStatsBySession.forEach { (alias, stats) ->
        gauge.labels(th2Pin, alias, FIRST.name).set(stats.sequence.toDouble())
        groupCounter.labels(th2Pin, alias, FIRST.name).inc(stats.groups.toDouble())
        messageCounter.labels(th2Pin, alias, FIRST.name, MESSAGE.name).inc(stats.messages.toDouble())
        messageCounter.labels(th2Pin, alias, FIRST.name, RAW_MESSAGE.name).inc(stats.rawMessages.toDouble())
    }

    outgoingStatsBySession.forEach { (alias, stats) ->
        gauge.labels(th2Pin, alias, SECOND.name).set(stats.sequence.toDouble())
        groupCounter.labels(th2Pin, alias, SECOND.name).inc(stats.groups.toDouble())
        messageCounter.labels(th2Pin, alias, SECOND.name, MESSAGE.name).inc(stats.messages.toDouble())
        messageCounter.labels(th2Pin, alias, SECOND.name, RAW_MESSAGE.name).inc(stats.rawMessages.toDouble())
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