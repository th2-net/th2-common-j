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
import com.exactpro.th2.common.schema.factory.LazyProvider.ThrowableConsumer
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * This is class is for internal use only. Please, keep that in mind when using it in another module
 */
// Kotlin internal keyword does not help here because you can still access the class from Java
class LazyProvider<T : Any?> private constructor(
    private val name: String,
    private val supplier: Callable<out T>,
    private val onClose: ThrowableConsumer<in T>
) : AutoCloseable {
    private val reference = AtomicReference<State<T>?>()

    init {
        require(name.isNotBlank()) { "blank name provided" }
    }

    fun get(): T {
        return getState().value
    }

    /**
     * The difference from [get] method is that this one does not throw an exception
     * if provider already closed.
     * However, it will still throw an exception if value initialization is failed
     */
    fun getOrNull(): T? {
        return getState().takeIf { it is State.Hold }?.value
    }

    private fun getState(): State<T> {
        return if (reference.compareAndSet(null, State.Init)) {
            val holder = State.Hold(initialize())
            if (!reference.compareAndSet(State.Init, holder)) {
                if (holder.value != null) {
                    onClose.consume(holder.value)
                }
                error("provider '$name' already closed")
            }
            holder
        } else {
            var currentState: State<T>?
            do {
                currentState = reference.get()
                Thread.yield()
            } while (currentState == State.Init || currentState == null)
            currentState
        }
    }

    @Throws(Exception::class)
    override fun close() {
        if (reference.compareAndSet(null, State.Closed)) {
            // factory was not yet initialized. Just return
            return
        }

        val prevState = reference.getAndSet(State.Closed)
        if (prevState is State.Hold && prevState.value != null) {
            onClose.consume(prevState.value)
        }
    }

    private fun initialize(): T {
        return try {
            supplier.call()
        } catch (e: Exception) {
            reference.set(State.Error)
            throw CommonFactoryException("cannot initialize '$name'", e)
        }
    }

    private fun interface ThrowableConsumer<T> {
        @Throws(Exception::class)
        fun consume(value: T)
    }

    private sealed class State<out T> {
        abstract val value: T
        object Init : State<Nothing>() {
            override val value: Nothing
                get() = error("no value on initialization")
        }
        class Hold<T>(override val value: T): State<T>()
        object Closed : State<Nothing>() {
            override val value: Nothing
                get() = error("no value after close call")
        }
        object Error : State<Nothing>() {
            override val value: Nothing
                get() = error("no value on initialization error")
        }
    }

    companion object {
        private val NONE: ThrowableConsumer<in Any?> = ThrowableConsumer { }

        @JvmStatic
        fun <T> lazy(name: String, supplier: Callable<T>): LazyProvider<T> =
            LazyProvider(name, supplier, NONE)

        @JvmStatic
        fun <T> lazyCloseable(name: String, supplier: Callable<T>, onClose: Consumer<T>): LazyProvider<T> =
            LazyProvider(name, supplier, onClose::accept)

        @JvmStatic
        fun <T : AutoCloseable> lazyAutocloseable(name: String, supplier: Callable<T>): LazyProvider<T> =
            LazyProvider(name, supplier, AutoCloseable::close)
    }
}