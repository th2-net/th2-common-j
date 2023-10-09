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
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock
import kotlin.system.exitProcess

/**
 * Factory can share closable resources between application. They should be closed on [IApplicationFactory.close] method call
 */
interface IApplicationFactory : AutoCloseable {
    /**
     * Creates onetime application.
     * If you need restart of reconfigure application, close old instance and create new one.
     */
    fun createApplication(context: ApplicationContext): IApplication

    /**
     * Close resources shared between created application.
     */
    override fun close() {}

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private const val APPLICATION_FACTORY_SYSTEM_PROPERTY = "th2.microservice.application-factory"
        private const val MONITOR_NAME = "microservice_main"
        @JvmStatic
        fun run(factory: IApplicationFactory?, vararg args: String) {
            val liveness: MetricMonitor = registerLiveness(MONITOR_NAME)
            val readiness: MetricMonitor = registerReadiness(MONITOR_NAME)

            val resources: Deque<Resource> = ConcurrentLinkedDeque()
            val lock = ReentrantLock()
            val condition: Condition = lock.newCondition()

            configureShutdownHook(resources, lock, condition, liveness, readiness)

            K_LOGGER.info { "Starting a component" }
            liveness.enable()

            K_LOGGER.info { "Preparing application by ${ factory?.javaClass ?: "internal factory" }" }

            val applicationFactory = factory ?: loadApplicationFactory().also {
                // add only loaded application factory to resources as internal resource
                resources.add(Resource("application-factory", it))
                K_LOGGER.info { "Loaded the ${it.javaClass} factory" }
            }

            val commonFactory = CommonFactory.createFromArguments(*args)
            resources.add(Resource("common-factory", commonFactory))

            val application = applicationFactory.createApplication(
                ApplicationContext(
                    commonFactory,
                    { name: String, closeable: AutoCloseable -> resources.add(Resource(name, closeable)) },
                    ::panic
                )
            )
            resources.add(Resource("application", application))

            K_LOGGER.info { "Starting the ${application.javaClass} application of the '${commonFactory.boxConfiguration.boxName}' component" }

            application.start()

            K_LOGGER.info { "Started the '${commonFactory.boxConfiguration.boxName}' component" }
            readiness.enable()

            awaitShutdown(lock, condition)
        }

        private fun panic(ex: Throwable?) {
            K_LOGGER.error(ex) { "Component panic exception" }
            exitProcess(2)
        }
        private fun configureShutdownHook(
            resources: Deque<Resource>,
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
                        resource.closable.close()
                    }.onFailure { e ->
                        K_LOGGER.error(e) { "Cannot close the ${resource.name} resource ${resource.closable::class}" }
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

        private class Resource(val name: String, val closable: AutoCloseable)
    }
}