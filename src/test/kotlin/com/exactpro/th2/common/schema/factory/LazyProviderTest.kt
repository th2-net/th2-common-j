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

package com.exactpro.th2.common.schema.factory

import com.exactpro.th2.common.schema.exception.CommonFactoryException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.inOrder
import org.mockito.kotlin.mock
import org.mockito.kotlin.mockingDetails
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.assertContains
import kotlin.test.assertNotNull

internal class LazyProviderTest {
    @RepeatedTest(50)
    fun `concurrent get invocation`() {
        val supplier: Callable<Int> = mock { }
        val provider: LazyProvider<Int> = LazyProvider.lazy("test", supplier)

        ForkJoinPool.commonPool().invokeAll(
            listOf(
                Callable { provider.get() },
                Callable { provider.get() },
                Callable { provider.get() },
                Callable { provider.get() },
                Callable { provider.get() },
            )
        )

        verify(supplier, times(1)).call()
    }

    @RepeatedTest(50)
    fun `concurrent close invocation`() {
        val resource: AutoCloseable = mock { }
        val supplier: Callable<AutoCloseable> = mock {
            on { call() } doReturn resource
        }
        val provider: LazyProvider<AutoCloseable> = LazyProvider.lazyAutocloseable("test", supplier)
        // init value
        provider.get()
        ForkJoinPool.commonPool().invokeAll(
            listOf(
                Callable { provider.close() },
                Callable { provider.close() },
                Callable { provider.close() },
                Callable { provider.close() },
                Callable { provider.close() },
                Callable { provider.close() },
            )
        )

        inOrder(resource, supplier) {
            verify(supplier, times(1)).call()
            verify(resource, times(1)).close()
        }
    }

    @RepeatedTest(100)
    fun `concurrent get and close invocation`() {
        val resource: AutoCloseable = mock { }
        val supplier: Callable<AutoCloseable> = mock {
            on { call() } doReturn resource
        }
        val provider: LazyProvider<AutoCloseable> = LazyProvider.lazyAutocloseable("test", supplier)

        val task = Callable {
            val i = ThreadLocalRandom.current().nextInt(100)
            if (i % 2 == 0) {
                provider.get()
            } else {
                provider.close()
            }
        }

        val results = ForkJoinPool.commonPool().invokeAll(
            listOf(
                task,
                task,
                task,
                task,
                // make sure at least one call to close method is done
                Callable { provider.close() },
            )
        )

        val details = mockingDetails(supplier)
        if (details.invocations.isEmpty()) {
            verify(supplier, never()).call()
            verify(resource, never()).close()
        } else {
            inOrder(supplier, resource) {
                verify(supplier, times(1)).call()
                verify(resource, times(1)).close()
            }
        }
        val errors = results.asSequence().mapNotNull {
            try {
                it.get()
                null
            } catch (ex: ExecutionException) {
                ex
            }
        }.toList()
        val possibleErrors = listOf(
            "no value after close call",
            "provider 'test' already closed",
        )
        Assertions.assertTrue(errors.all { ex ->
            val errorMessage: String = ex.cause!!.message!!
            possibleErrors.any { errorMessage.endsWith(it) }
        }) {
            "unexpected errors thrown: $errors"
        }
    }

    @Test
    fun `null resources`() {
        val provider = LazyProvider.lazy<Int?>("test") { null }
        Assertions.assertEquals(null, provider.get(), "unexpected value")
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `exception in supplier`(safeCall: Boolean) {
        val provider = LazyProvider.lazy<Unit>("test") { error("supplier error") }
        val ex = assertThrows<CommonFactoryException> {
            if (safeCall) provider.getOrNull() else provider.get()
        }
        assertNotNull(ex.cause?.message, "no cause") {
            assertContains(it, "supplier error")
        }
    }
}