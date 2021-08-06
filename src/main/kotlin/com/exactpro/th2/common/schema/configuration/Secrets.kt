package com.exactpro.th2.common.schema.configuration

import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter

class Secrets {
    private val secrets = mutableMapOf<String, String>()
    @JsonAnyGetter get() = field

    operator fun get(name: String): String? = secrets[name]
    @JsonAnySetter private fun set(name: String, value: String) = secrets.set(name, value)
    override fun toString(): String = secrets.toString()
}