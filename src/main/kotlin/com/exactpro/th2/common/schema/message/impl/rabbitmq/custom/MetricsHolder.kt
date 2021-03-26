/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.custom

import com.exactpro.th2.common.metrics.DEFAULT_BUCKETS
import com.google.common.base.CaseFormat
import io.prometheus.client.Counter
import io.prometheus.client.Histogram

class MetricsHolder(
    customTag: String
) {
    val processingDeliveryHistogram: Histogram = registerProcessingDescribable("$customTag delivery");
    val processingDataHistogram: Histogram = registerProcessingDescribable(customTag)

    val processingDataFailureCounter: Counter = registerProcessingFailureDescribable(customTag)

    val outgoingDeliveryCounter: Counter = registerOutgoingQuantityDescribable("$customTag delivery")
    val outgoingDataCounter: Counter = registerOutgoingQuantityDescribable(customTag)

    companion object {
        fun registerProcessingDescribable(customTag: String): Histogram = Histogram.build()
            .buckets(*DEFAULT_BUCKETS)
            .name(prepareName("th2_mq_${customTag}_processing_time"))
            .help("Time spent to process a $customTag")
            .register()

        fun registerProcessingFailureDescribable(customTag: String): Counter = Counter.build()
            .name(prepareName("th2_mq_${customTag}_processing_failure_quantity"))
            .help("The number of a $customTag processing failure")
            .register()

        fun registerOutgoingQuantityDescribable(customTag: String): Counter = Counter.build()
            .name(prepareName("th2_mq_outgoing_${customTag}_quantity"))
            .help("The number of a $customTag sent")
            .register()

        private fun prepareName(name: String): String = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name.decapitalize())
    }
}