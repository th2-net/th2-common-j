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
 * Instance of one time application which works from the [IApplication.start] to [IApplication.close] method.
 */
interface IApplication: AutoCloseable {
    /**
     * Starts one time application.
     * This method can be called only once.
     * @exception IllegalStateException can be thrown when:
     *   * the method is called the two or more time
     *   * close method has been called before
     *   * other reasons
     */
    fun start()
}