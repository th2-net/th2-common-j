package com.exactpro.th2.common.schema.filter.strategy.impl

import com.exactpro.th2.common.schema.message.configuration.FieldFilterConfiguration
import com.exactpro.th2.common.schema.message.configuration.FieldFilterOperation
import org.apache.commons.io.FilenameUtils

class FieldValueChecker {
    fun check(value: String?, filterConfiguration: FieldFilterConfiguration): Boolean {
        val valueInConf = filterConfiguration.expectedValue
        return when (filterConfiguration.operation) {
            FieldFilterOperation.EQUAL -> value == valueInConf
            FieldFilterOperation.NOT_EQUAL -> value != valueInConf
            FieldFilterOperation.EMPTY -> value.isNullOrEmpty()
            FieldFilterOperation.NOT_EMPTY ->  !value.isNullOrEmpty()
            FieldFilterOperation.WILDCARD -> FilenameUtils.wildcardMatch(value, valueInConf)
        }
    }
}