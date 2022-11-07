/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.metrics.DIRECTION_LABEL
import com.exactpro.th2.common.metrics.MESSAGE_TYPE_LABEL
import com.exactpro.th2.common.metrics.SESSION_ALIAS_LABEL
import com.exactpro.th2.common.metrics.TH2_PIN_LABEL
import com.exactpro.th2.common.metrics.incrementDroppedMetrics
import com.exactpro.th2.common.metrics.incrementTotalMetrics
import com.exactpro.th2.common.metrics.incrementTotalMetricsOld
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode.AverageTime
import org.openjdk.jmh.annotations.Mode.Throughput
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Scope.Thread
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State

@State(Scope.Benchmark)
open class MetricsUtilsBenchmark {


    @State(Thread)
    open class BatchState {
        lateinit var batch: MessageGroupBatch
        lateinit var messages: List<AnyMessage>

        lateinit var messageCounter: Counter
        lateinit var groupCounter: Counter
        lateinit var groupSequenceGauge: Gauge

        @Setup
        open fun init() {
            messages = batch.groupsList.asSequence()
                .map(MessageGroup::getMessagesList)
                .flatMap(List<AnyMessage>::asSequence)
                .toList()

            messageCounter = Counter.build("message_counter", "Message counter")
                .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL, MESSAGE_TYPE_LABEL)
                .register()

            groupCounter = Counter.build("group_counter", "Group counter")
                .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
                .register()

            groupSequenceGauge = Gauge.build("group_sequence", "Group sequence")
                .labelNames(TH2_PIN_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
                .register()

            println("groups ${batch.groupsCount}, messages ${batch.groupsList.asSequence().map(MessageGroup::getMessagesList).map(List<AnyMessage>::size).sum()}")
        }

    }

    open class Simple: BatchState() {

        override fun init() {
            batch = MessageGroupBatch.newBuilder().apply {
                repeat(GROUP_IN_BATCH) {
                    addGroupsBuilder().apply {
                        repeat(MESSAGES_IN_GROUP) {
                            addMessagesBuilder().apply {
                                rawMessageBuilder.apply {
                                    metadataBuilder.apply {
                                        idBuilder.apply {
                                            direction = Direction.FIRST
                                            sequence = SEQUENCE_GENERATOR.next()
                                            connectionIdBuilder.apply {
                                                sessionAlias = ALIAS
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }.build()
            super.init()
        }
    }

    open class Multiple: BatchState() {
        override fun init() {
            batch = MessageGroupBatch.newBuilder().apply {
                repeat(GROUP_IN_BATCH) {
                    addGroupsBuilder().apply {
                        repeat(MESSAGES_IN_GROUP) {
                            addMessagesBuilder().apply {
                                rawMessageBuilder.apply {
                                    metadataBuilder.apply {
                                        idBuilder.apply {
                                            direction = DIRECTION_GENERATOR.next()
                                            sequence = SEQUENCE_GENERATOR.next()
                                            connectionIdBuilder.apply {
                                                sessionAlias = ALIAS_GENERATOR.next()
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }.build()
            super.init()
        }
    }

    @Benchmark
    @BenchmarkMode(Throughput, AverageTime)
    fun benchmarkIncrementTotalMetricsOldVsSimpleBatch(state: Simple) {
        incrementTotalMetricsOld(
            state.batch,
            TH2_PIN,
            state.messageCounter,
            state.groupCounter,
            state.groupSequenceGauge
        )
    }

    @Benchmark
    @BenchmarkMode(Throughput, AverageTime)
    fun benchmarkIncrementTotalMetricsOldVsMultipleBatch(state: Multiple) {
        incrementTotalMetricsOld(
            state.batch,
            TH2_PIN,
            state.messageCounter,
            state.groupCounter,
            state.groupSequenceGauge
        )
    }

    @Benchmark
    @BenchmarkMode(Throughput, AverageTime)
    fun benchmarkIncrementTotalMetricsVsSimpleBatch(state: Simple) {
        incrementTotalMetrics(
            state.batch,
            TH2_PIN,
            state.messageCounter,
            state.groupCounter,
            state.groupSequenceGauge
        )
    }

    @Benchmark
    @BenchmarkMode(Throughput, AverageTime)
    fun benchmarkIncrementTotalMetricsVsMultipleBatch(state: Multiple) {
        incrementTotalMetrics(
            state.batch,
            TH2_PIN,
            state.messageCounter,
            state.groupCounter,
            state.groupSequenceGauge
        )
    }

    @Benchmark
    @BenchmarkMode(Throughput, AverageTime)
    fun benchmarkIncrementDroppedMetricsVsSimpleBatch(state: Simple) {
        incrementDroppedMetrics(
            state.messages,
            TH2_PIN,
            state.messageCounter,
            state.groupCounter,
        )
    }

    @Benchmark
    @BenchmarkMode(Throughput, AverageTime)
    fun benchmarkIncrementDroppedMetricsVsMultipleBatch(state: Multiple) {
        incrementDroppedMetrics(
            state.messages,
            TH2_PIN,
            state.messageCounter,
            state.groupCounter,
        )
    }

    companion object {
        private const val TH2_PIN = "pin"
        private const val ALIAS = "alias"

        private const val ALIASES = 100
        private const val GROUP_IN_BATCH = 3_000
        private const val MESSAGES_IN_GROUP = 1

        private val SEQUENCE_GENERATOR = generateSequence(1L) { it + 1 }
            .iterator()

        private val ALIAS_GENERATOR = sequence {
            var counter = 0
            while (true) {
                yield(counter++ % ALIASES + 1)
            }
        }.map { ALIAS + it }
            .iterator()

        private val DIRECTION_GENERATOR = generateSequence(1) { it + 1 }
            .map { if (it % 2 == 0) Direction.FIRST else Direction.SECOND }
            .iterator()
    }
}