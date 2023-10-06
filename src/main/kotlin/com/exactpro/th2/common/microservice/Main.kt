/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.common.microservice

import com.exactpro.th2.common.metrics.MetricMonitor
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.metrics.registerReadiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import mu.KotlinLogging
import java.util.Deque
import java.util.ServiceLoader
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.system.exitProcess

private val K_LOGGER = KotlinLogging.logger {}

class Main(args: Array<String>) {

    private val liveness: MetricMonitor = registerLiveness(MONITOR_NAME)
    private val readiness: MetricMonitor = registerReadiness(MONITOR_NAME)

    private val resources: Deque<AutoCloseable> = ConcurrentLinkedDeque()
    private val lock = ReentrantLock()
    private val condition: Condition = lock.newCondition()

    private val commonFactory: CommonFactory
    private val application: IApplication

    init {
        configureShutdownHook(resources, lock, condition, liveness, readiness)

        K_LOGGER.info { "Starting a component" }
        liveness.enable()

        val applicationFactory = loadApplicationFactory()
            .also(resources::add)
        commonFactory = CommonFactory.createFromArguments(*args)
            .also(resources::add)
        application = applicationFactory.createApplication(ApplicationContext(commonFactory, resources::add, ::panic))
            .also(resources::add)

        K_LOGGER.info { "Prepared the '${commonFactory.boxConfiguration.boxName}' component" }
    }

    fun run() {
        K_LOGGER.info { "Starting the '${commonFactory.boxConfiguration.boxName}' component" }

        application.start()

        K_LOGGER.info { "Started the '${commonFactory.boxConfiguration.boxName}' component" }
        readiness.enable()

        awaitShutdown(lock, condition)
    }

    companion object {
        private const val APPLICATION_FACTORY_SYSTEM_PROPERTY = "th2.microservice.application-factory"
        private const val MONITOR_NAME = "microservice_main"
        private fun loadApplicationFactory(): IApplicationFactory {
            val instances = ServiceLoader.load(IApplicationFactory::class.java).toList()
            return when (instances.size) {
                0 -> error("No instances of ${IApplicationFactory::class.simpleName}")
                1 -> instances.single().also { single ->
                    System.getProperty(APPLICATION_FACTORY_SYSTEM_PROPERTY)?.let { value ->
                        check(value == single::class.qualifiedName) {
                            "Found instance of ${IApplicationFactory::class.simpleName} mismatches the class specified by $APPLICATION_FACTORY_SYSTEM_PROPERTY system property," +
                                    "configured: $value, found: ${single::class.qualifiedName}"
                        }
                    }
                }

                else -> {
                    System.getProperty(APPLICATION_FACTORY_SYSTEM_PROPERTY)?.let { value ->
                        instances.find { value == it::class.qualifiedName }
                            ?: error(
                                "Found instances of ${IApplicationFactory::class.simpleName} mismatches the class specified by $APPLICATION_FACTORY_SYSTEM_PROPERTY system property," +
                                        "configured: $value, found: ${instances.map { Object::class.qualifiedName }}"
                            )
                    } ?: error(
                        "More than 1 instance of ${IApplicationFactory::class.simpleName} has been found " +
                                "and $APPLICATION_FACTORY_SYSTEM_PROPERTY system property isn't specified," +
                                "instances: $instances"
                    )
                }
            }
        }
    }
}

fun main(args: Array<String>) {
    try {
        Main(args).run()
    } catch (ex: Exception) {
        K_LOGGER.error(ex) { "Cannot start the box" }
        exitProcess(1)
    }
}
fun panic(ex: Throwable?) {
    K_LOGGER.error(ex) { "Component panic exception" }
    exitProcess(2)
}
fun configureShutdownHook(
    resources: Deque<AutoCloseable>,
    lock: ReentrantLock,
    condition: Condition,
    liveness: MetricMonitor,
    readiness: MetricMonitor,
) {
    Runtime.getRuntime().addShutdownHook(thread(
        start = false,
        name = "Shutdown-hook"
    ) {
        K_LOGGER.info { "Shutdown start" }
        readiness.disable()
        lock.withLock { condition.signalAll() }
        resources.descendingIterator().forEachRemaining { resource ->
            runCatching {
                resource.close()
            }.onFailure { e ->
                K_LOGGER.error(e) { "Cannot close resource ${resource::class}" }
            }
        }
        liveness.disable()
        K_LOGGER.info { "Shutdown end" }
    })
}
@Throws(InterruptedException::class)
fun awaitShutdown(lock: ReentrantLock, condition: Condition) {
    lock.withLock {
        K_LOGGER.info { "Wait shutdown" }
        condition.await()
        K_LOGGER.info { "App shutdown" }
    }
}