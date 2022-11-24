/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.util

import kotlin.random.Random
import org.testcontainers.containers.Container
import org.testcontainers.containers.RabbitMQContainer

class RabbitTestContainerUtil {
    companion object {
        fun declareQueue(rabbit: RabbitMQContainer, queueName: String): Container.ExecResult? {
            return execCommandWithSplit(rabbit, "rabbitmqadmin declare queue name=$queueName durable=false")

        }

        fun declareFanoutExchangeWithBinding(
            rabbit: RabbitMQContainer,
            exchangeName: String,
            destinationQueue: String
        ) {
            execCommandWithSplit(rabbit, "rabbitmqadmin declare exchange name=$exchangeName type=fanout")
            execCommandWithSplit(
                rabbit,
                "rabbitmqadmin declare binding source=$exchangeName destination_type=queue destination=$destinationQueue"
            )
        }

        fun putMessageInQueue(rabbit: RabbitMQContainer, queueName: String): Container.ExecResult? {
            return execCommandWithSplit(
                rabbit,
                """rabbitmqadmin publish exchange=amq.default routing_key=$queueName payload="hello-${
                    Random.nextInt(
                        0,
                        1000
                    )
                }""""
            )
        }

        fun getQueuesInfo(rabbit: RabbitMQContainer): Container.ExecResult? {
            return execCommandWithSplit(rabbit, "rabbitmqctl list_queues")
        }

        fun getChannelsInfo(rabbit: RabbitMQContainer): String {
            return execCommandWithSplit(rabbit, "rabbitmqctl list_consumers").toString()

        }

        fun getSubscribedChannelsCount(
            rabbitMQContainer: RabbitMQContainer,
            queue: String,
        ): Int {
            val channelsInfo = getChannelsInfo(rabbitMQContainer)
            return channelsInfo.countMatches(queue)
        }

        fun restartContainer(rabbit: RabbitMQContainer) {
            val tag: String = rabbit.containerId
            val snapshotId: String = rabbit.dockerClient.commitCmd(tag)
                .withRepository("temp-rabbit")
                .withTag(tag).exec()
            rabbit.stop()
            rabbit.dockerImageName = snapshotId
            rabbit.start()
        }

        private fun execCommandWithSplit(rabbit: RabbitMQContainer, command: String): Container.ExecResult? {
            return rabbit.execInContainer(
                *command.split(" ").toTypedArray()
            )
        }

        private fun String.countMatches(pattern: String): Int {
            return this.substringAfter("active\targuments")
                .split(pattern)
                .dropLastWhile { s -> s.isEmpty() }
                .toTypedArray().size - 1
        }

    }
}