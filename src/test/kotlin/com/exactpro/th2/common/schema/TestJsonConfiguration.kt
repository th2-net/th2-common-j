/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.metrics.PrometheusConfiguration
import com.exactpro.th2.common.schema.cradle.CradleConfidentialConfiguration
import com.exactpro.th2.common.schema.cradle.CradleNonConfidentialConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcEndpointConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcRawRobinStrategy
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServerConfiguration
import com.exactpro.th2.common.schema.grpc.configuration.GrpcServiceConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.MessageRouterConfiguration
import com.exactpro.th2.common.schema.message.configuration.MqRouterFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.QueueConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.ConnectionManagerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration.RabbitMQConfiguration
import com.exactpro.th2.common.schema.strategy.route.RoutingStrategy
import com.exactpro.th2.common.schema.strategy.route.impl.RobinRoutingStrategy
import com.exactpro.th2.common.schema.strategy.route.json.JsonDeserializerRoutingStategy
import com.exactpro.th2.common.schema.strategy.route.json.JsonSerializerRoutingStrategy
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestJsonConfiguration {

    companion object {
        @JvmStatic
        private val OBJECT_MAPPER: ObjectMapper = ObjectMapper()

        init {
            OBJECT_MAPPER.registerModule(KotlinModule())

            val routingStrategyModule = SimpleModule()
            routingStrategyModule.addDeserializer(RoutingStrategy::class.java, JsonDeserializerRoutingStategy())
            routingStrategyModule.addSerializer(RoutingStrategy::class.java, JsonSerializerRoutingStrategy(OBJECT_MAPPER))

            OBJECT_MAPPER.registerModule(routingStrategyModule)
        }
    }

    @Test
    fun `test grpc json configuration`() {
        testJson(GrpcConfiguration(
            mapOf(
                "test" to GrpcServiceConfiguration(
                    RobinRoutingStrategy().apply {
                        init(GrpcRawRobinStrategy().also { it.endpoints = listOf("endpoint") })
                    },
                    GrpcConfiguration::class.java,
                    mapOf("endpoint" to GrpcEndpointConfiguration("host", 12345, listOf("test_attr")))
                )
            ),
            GrpcServerConfiguration("host123", 1234)
        ))
    }

    @Test
    fun `test rabbitmq json configuration`() {
        testJson(RabbitMQConfiguration("host",
            "vHost",
            1234,
            "user",
            "pass",
            "subscriberName",
            "exchangeName"))
    }

    @Test
    fun `test connection manager json configuration`() {
        testJson(ConnectionManagerConfiguration("subscriberName", 10000))
    }

    @Test
    fun `test router mq json configuration`(){
        testJson(MessageRouterConfiguration(mapOf("test_queue" to QueueConfiguration(
            "routing_key",
            "queue_name",
            "exchange",
            listOf("attr1", "attr2"),
            listOf(MqRouterFilterConfiguration(mapOf("session_alias" to FieldFilterConfiguration("test_session_alias", FilterOperation.EQUAL)),
                mapOf("test_field" to FieldFilterConfiguration("test_value", FilterOperation.EQUAL))))
        ))))
    }

    @Test
    fun `test cradle confidential json configuration`() {
        testJson(CradleConfidentialConfiguration("data center",
            "host",
            "keyspace",
            1234,
            "user",
            "pass",
            "instance"))
    }

    @Test
    fun `test backward compatibility for grpc json configuration`() {
        OBJECT_MAPPER.readValue(
            "{\"server\":{\"host\":null,\"port\":8080,\"workers\":5},\"services\":{\"check1Service\":{\"endpoints\":{\"th2-qa-endpoint\":{\"host\":\"localhost\",\"port\":31304}},\"service-class\":\"${GrpcServiceConfiguration::class.java.name}\",\"strategy\":{\"endpoints\":[\"th2-qa-endpoint\"],\"name\":\"robin\"}}}}",
            GrpcConfiguration::class.java)
    }

    @Test
    fun `test backward compatibility for rabbitmq json configuration`() {
        OBJECT_MAPPER.readValue(
            "{\"host\":\"localhost\",\"vHost\":\"schema\",\"port\":\"5672\",\"username\":\"schema\",\"password\":\"RABBITMQ_PASS\",\"exchangeName\":\"exchange\"}",
            RabbitMQConfiguration::class.java)
    }

    @Test
    fun `test backward compatibility for router mq json configuration`() {
        OBJECT_MAPPER.readValue(
            "{\"queues\":{\"estore-pin\":{\"attributes\":[\"publish\",\"event\"],\"exchange\":\"demo_exchange\",\"filters\":[],\"name\":\"key[schema-dev:act-dev-entry-point:estore-pin]\",\"queue\":\"\"},\"from_codec\":{\"attributes\":[\"subscribe\",\"oe\",\"parsed\",\"first\"],\"exchange\":\"demo_exchange\",\"filters\":[],\"name\":\"\",\"queue\":\"link[schema-dev:act-dev-entry-point:from_codec]\"},\"some_pin\":{\"attributes\":[],\"exchange\":\"demo_exchange\",\"filters\":[],\"name\":\"\",\"queue\":\"link[schema-dev:act-dev-entry-point:some_pin]\"},\"to_send_codec\":{\"attributes\":[\"subscribe\",\"oe\",\"parsed\",\"first\"],\"exchange\":\"demo_exchange\",\"filters\":[{\"message\":{},\"metadata\":{\"session_alias\":{\"operation\":\"EQUAL\",\"value\":\"codec-fix\"}}}],\"name\":\"\",\"queue\":\"link[schema-dev:act-dev-entry-point:to_send_codec]\"}}}",
            MessageRouterConfiguration::class.java)
    }

    @Test
    fun `test backward compatibility for cradle confidential json configuration`() {
        OBJECT_MAPPER.readValue(
            "{\"dataCenter\":\"datacenter1\",\"host\":\"localhost\",\"port\":\"9042\",\"keyspace\":\"schema_dev\",\"username\":\"th2\",\"password\":\"CASSANDRA_PASS\"}",
            CradleConfidentialConfiguration::class.java
        )
    }
    @Test
    fun `test cradle non confidential json configuration`() {
        testJson(CradleNonConfidentialConfiguration())
    }

    @Test
    fun `test prometheus confidential json configuration`() {
        testJson(PrometheusConfiguration())
        OBJECT_MAPPER.readValue(
            "{\"enabled\":\"true\"}",
            PrometheusConfiguration::class.java)
    }

    private fun testJson(configuration: Any) {
        OBJECT_MAPPER.writeValueAsString(configuration).also {
            assertEquals(configuration, OBJECT_MAPPER.readValue(it, configuration::class.java))
        }
    }
}