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
}