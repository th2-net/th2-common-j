package com.exactpro.th2.common.schema.message

import org.testcontainers.utility.DockerImageName
import java.time.Duration

class ContainerConstants {
    companion object {
        val RABBITMQ_IMAGE_NAME: DockerImageName = DockerImageName.parse("rabbitmq:3.12.7-management-alpine")
        const val ROUTING_KEY = "routingKey"
        const val QUEUE_NAME = "queue"
        const val EXCHANGE = "test-exchange"

        const val DEFAULT_PREFETCH_COUNT = 10
        val DEFAULT_CONFIRMATION_TIMEOUT: Duration = Duration.ofSeconds(1)
    }
}