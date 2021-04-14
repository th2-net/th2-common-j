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
            val module = SimpleModule()
            module.addDeserializer(RoutingStrategy::class.java, JsonDeserializerRoutingStategy())
            module.addSerializer(RoutingStrategy::class.java, JsonSerializerRoutingStrategy(ObjectMapper().apply { registerModule(KotlinModule()) }))

            OBJECT_MAPPER.registerModule(module)
            OBJECT_MAPPER.registerModule(KotlinModule())
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
    fun `test cradle non confidential json configuration`() {
        testJson(CradleNonConfidentialConfiguration())
    }

    fun testJson(configuration: Any) {
        OBJECT_MAPPER.writeValueAsString(configuration).also {
            assertEquals(configuration, OBJECT_MAPPER.readValue(it, configuration::class.java))
        }
    }
}