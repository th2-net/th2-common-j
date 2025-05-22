/*
 *  Copyright 2025 Exactpro (Exactpro Systems Limited)
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.common.rabbitmq

import com.rabbitmq.http.client.Client
import com.rabbitmq.http.client.ClientParameters
import java.net.URL

// FIXME: infra-operator should grant privileges for user
class RabbitMQInformer private constructor(
    host: String,
    port: Int,
    username: String,
    password: String,
) {
    private val client = Client(
        ClientParameters()
            .url(URL(RABBITMQ_PROTOCOL, host, port, RABBITMQ_API_URL_PART))
            .username(username)
            .password(password)
    )

    init {
        try {
            client.vhosts
        } catch (e: Exception) {
            throw IllegalArgumentException(
                "RabbitMQ REST API isn't accessible by $RABBITMQ_PROTOCOL://$host:$port url", e
            )
        }
    }

    /**
     * Gets actual [com.exactpro.th2.common.rabbitmq.QueueInfo] for all queues related to the vHost and passed filter
     * @param vHost for getting actual data.
     * @param filter for queue name. All queues are passed default filter
     */
    @JvmOverloads
    fun getQueuesInfo(vHost: String, filter: (String) -> Boolean = { true }): List<QueueInfo> {
        return client.getQueues(vHost).asSequence()
            .filter { queue -> filter.invoke(queue.name) }
            .map { queue ->
                QueueInfo(
                    vHost,
                    queue.name,
                    queue.messagesReady,
                    queue.messagesUnacknowledged,
                    queue.totalMessages,
                    queue.consumerCount,
                )
            }.toList()
    }

    companion object {
        private const val RABBITMQ_PROTOCOL: String = "http"
        private const val RABBITMQ_API_URL_PART: String = "/api"

        /**
         * User must have at list `monitoring` privileges for correct work.
         * More details by the link: https://www.rabbitmq.com/docs/management#permissions
         */
        fun create(
            host: String,
            port: Int,
            username: String,
            password: String,
        ) = RabbitMQInformer(host, port, username, password)
    }
}

data class QueueInfo(
    val vHost: String,
    val name: String,
    val messagesReady: Long,
    val messagesUnacknowledged: Long,
    val totalMessages: Long,
    val consumerCount: Long
)