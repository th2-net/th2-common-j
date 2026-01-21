/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema

import com.exactpro.cradle.cassandra.CassandraStorageSettings
import com.exactpro.th2.common.metrics.PrometheusConfiguration
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import com.exactpro.th2.common.schema.grpc.configuration.*
import com.exactpro.th2.common.schema.message.configuration.*
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.strategy.route.impl.RobinRoutingStrategy
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.shaded.org.apache.commons.lang3.builder.EqualsBuilder
import java.nio.file.Path

class TestJsonConfiguration {

    @Test
    fun `test grpc json configuration deserialize`() {
        testDeserialize(GRPC_CONF_JSON, GRPC_CONF)
    }

    @Test
    fun `test grpc router json configuration deserialize`() {
        testDeserialize(GRPC_ROUTER_CONF_JSON, GRPC_ROUTER_CONF)
    }

    @Test
    fun `test grpc json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(GRPC_CONF)
    }

    @Test
    fun `test rabbitmq json configuration deserialize`() {
        testDeserialize(RABBITMQ_CONF_JSON, RABBITMQ_CONF)
    }

    @Test
    fun `test rabbitmq json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(RABBITMQ_CONF)
    }

    @Test
    fun `test connection manager json configuration deserialize`() {
        testDeserialize(CONNECTION_MANAGER_CONF_JSON, CONNECTION_MANAGER_CONF)
    }

    @Test
    fun `test connection manager json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(CONNECTION_MANAGER_CONF)
    }

    @Test
    fun `test router mq json configuration deserialize`() {
        testDeserialize(MESSAGE_ROUTER_CONF_JSON, MESSAGE_ROUTER_CONF)
    }

    @Test
    fun `test router mq json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(MESSAGE_ROUTER_CONF)
    }

    @Test
    fun `test cradle confidential json configuration deserialize`() {
        testDeserialize(CRADLE_CONFIDENTIAL_CONF_JSON, CRADLE_CONFIDENTIAL_CONF)
    }

    @Test
    fun `test cradle confidential json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(CRADLE_CONFIDENTIAL_CONF)
    }

    @Test
    fun `test cradle non confidential json configuration deserialize`() {
        testDeserialize(CRADLE_NON_CONFIDENTIAL_CONF_JSON, CRADLE_NON_CONFIDENTIAL_CONF)
    }

    @Test
    fun `test cassandra storage settings json configuration deserialize`() {
        assertTrue(EqualsBuilder.reflectionEquals(MAPPER.readValue(CASSANDRA_STORAGE_SETTINGS_JSON, CASSANDRA_STORAGE_SETTINGS::class.java), CASSANDRA_STORAGE_SETTINGS))
    }

    @Test
    fun `test cradle non confidential json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(CRADLE_NON_CONFIDENTIAL_CONF)
    }

    @Test
    fun `test json configuration deserialize combo`() {
        assertTrue(EqualsBuilder.reflectionEquals(CASSANDRA_STORAGE_SETTINGS, MAPPER.readValue(CRADLE_NON_CONFIDENTIAL_COMBO_CONF_JSON, CASSANDRA_STORAGE_SETTINGS::class.java)))
        assertEquals(CRADLE_NON_CONFIDENTIAL_CONF, MAPPER.readValue(CRADLE_NON_CONFIDENTIAL_COMBO_CONF_JSON, CRADLE_NON_CONFIDENTIAL_CONF::class.java))
    }

    @Test
    fun `test prometheus confidential json configuration deserialize`() {
        testDeserialize(PROMETHEUS_CONF_JSON, PROMETHEUS_CONF)
    }

    @Test
    fun `test prometheus confidential json configuration serialize and deserialize`() {
        testSerializeAndDeserialize(PROMETHEUS_CONF)
    }

    private fun testSerializeAndDeserialize(configuration: Any) {
        MAPPER.writeValueAsString(configuration).also { jsonString ->
            testDeserialize(jsonString, configuration)
        }
    }

    private fun testDeserialize(json: String, obj: Any) {
        assertEquals(obj, MAPPER.readValue(json, obj::class.java))
    }

    companion object {
        @JvmStatic
        private val CONF_DIR = Path.of("test_json_configurations")

        private val GRPC_CONF_JSON = loadConfJson("grpc")
        private val GRPC_CONF = GrpcConfiguration(
            mapOf(
                "test" to GrpcServiceConfiguration(
                    RobinRoutingStrategy().apply {
                        init(GrpcRawRobinStrategy(listOf("endpoint")))
                    },
                    GrpcConfiguration::class.java,
                    mapOf("endpoint" to GrpcEndpointConfiguration("host", 12345, attributes = listOf("test_attr"))),
                    emptyList()
                )
            ),
            GrpcServerConfiguration("host123", 1234, 58),
        )

        private val GRPC_ROUTER_CONF_JSON = loadConfJson("grpc_router")
        private val GRPC_ROUTER_CONF = GrpcRouterConfiguration(
            true,
            61,
            4194305,
            GrpcRetryConfiguration(
                61,
                101,
                120001
            )
        )

        private val RABBITMQ_CONF_JSON = loadConfJson("rabbitMq")
        private val RABBITMQ_CONF = RabbitMQConfiguration(
            "host",
            "vHost",
            1234,
            "user",
            "pass",
            "subscriberName",
            "exchangeName"
        )

        private val CONNECTION_MANAGER_CONF_JSON = loadConfJson("connection_manager")
        private val CONNECTION_MANAGER_CONF = ConnectionManagerConfiguration(
            "subscriberName",
            10000,
            12000,
            8,
            8888,
            88888,
            1
        )

        private val MESSAGE_ROUTER_CONF_JSON = loadConfJson("message_router")
        private val MESSAGE_ROUTER_CONF = MessageRouterConfiguration(
            mapOf("test_queue" to QueueConfiguration(
                "routing_key",
                "queue_name",
                "exchange",
                listOf("attr1", "attr2")
            ).apply {
                filters = listOf(
                    MqRouterFilterConfiguration(
                        listOf(
                            FieldFilterConfiguration(
                                "session_alias",
                                "test_session_alias",
                                FieldFilterOperation.EQUAL
                            )
                        ),
                        listOf(FieldFilterConfiguration("test_field", "test_value", FieldFilterOperation.EQUAL))
                    ),
                    MqRouterFilterConfiguration(
                        listOf(
                            FieldFilterConfiguration(
                                "session_alias",
                                "test_session_alias",
                                FieldFilterOperation.EQUAL
                            )
                        ),
                        listOf(
                            FieldFilterConfiguration("test_field", "test_value0", FieldFilterOperation.EQUAL),
                            FieldFilterConfiguration("test_field", "test_value1", FieldFilterOperation.EQUAL)
                        )
                    )
                )
            }),
            GlobalNotificationConfiguration()
        )

        private val CRADLE_CONFIDENTIAL_CONF_JSON = loadConfJson("cradle_confidential")
        private val CRADLE_CONFIDENTIAL_CONF = CradleConfidentialConfiguration(
            "data center",
            "host",
            "keyspace",
            1234,
            "user",
            "pass"
        )

        private val CRADLE_NON_CONFIDENTIAL_COMBO_CONF_JSON = loadConfJson("cradle_non_confidential_combo")
        private val CRADLE_NON_CONFIDENTIAL_CONF_JSON = loadConfJson("cradle_non_confidential")
        private val CRADLE_NON_CONFIDENTIAL_CONF = CradleNonConfidentialConfiguration(
            false,
            111,
            123,
            321,
            5000,
            1280002,
        )
        private val CASSANDRA_STORAGE_SETTINGS_JSON = loadConfJson("cassandra_storage_settings")
        private val CASSANDRA_STORAGE_SETTINGS = CassandraStorageSettings().apply {
//            networkTopologyStrategy = NetworkTopologyStrategyBuilder()
//                .add("A", 3)
//                .add("B", 3)
//                .build()
            timeout = 4999
//            writeConsistencyLevel = ConsistencyLevel.THREE
//            readConsistencyLevel = ConsistencyLevel.QUORUM
            keyspace = "test-keyspace"
            keyspaceReplicationFactor = 1
            maxParallelQueries = 1
            resultPageSize = 2
            maxMessageBatchSize = 3
            maxUncompressedMessageBatchSize = 5
            maxTestEventBatchSize = 8
            maxUncompressedTestEventSize = 13
            sessionsCacheSize = 21
            scopesCacheSize = 34
            pageSessionsCacheSize = 55
            pageScopesCacheSize = 89
            sessionStatisticsCacheSize = 144
            pageGroupsCacheSize = 233
            groupsCacheSize = 377
            eventBatchDurationCacheSize = 610
            counterPersistenceInterval = 987
            composingServiceThreads = 1597
        }

        private val PROMETHEUS_CONF_JSON = loadConfJson("prometheus")
        private val PROMETHEUS_CONF = PrometheusConfiguration("123.3.3.3", 1234, false)

        private fun loadConfJson(fileName: String): String {
            val path = CONF_DIR.resolve(fileName)

            return Thread.currentThread().contextClassLoader
                .getResourceAsStream("$path.json")?.readAllBytes()?.let { bytes -> String(bytes) }
                ?: error("Can not load resource by path $path.json")
        }
    }
}