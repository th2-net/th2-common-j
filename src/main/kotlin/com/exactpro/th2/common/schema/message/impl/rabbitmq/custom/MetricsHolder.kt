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
    val incomingDeliveryCounter: Counter
    val incomingDataCounter: Counter
    val processingTimer: Histogram
    val outgoingDeliveryCounter: Counter
    val outgoingDataCounter: Counter

    init {
        val tag = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, customTag.decapitalize())

        incomingDeliveryCounter = Counter.build()
            .name("th2_mq_incoming_${tag}_delivery_quantity")
            .help("The received Rabbit MQ delivery quantity")
            .register()
        incomingDataCounter = Counter.build()
            .name("th2_mq_incoming_${tag}_data_quantity")
            .help("The received data quantity")
            .register()
        outgoingDeliveryCounter = Counter.build()
            .name("th2_mq_outgoing_${tag}_delivery_quantity")
            .help("The number of sent Rabbit MQ messages")
            .register()
        outgoingDataCounter = Counter.build()
            .name("th2_mq_outgoing_${tag}_data_quantity")
            .help("The number of sent messages")
            .register()
        processingTimer = Histogram.build()
            .buckets(*DEFAULT_BUCKETS)
            .name("th2_mq_${tag}_processing_time")
            .help("Time spent to process a single delivery")
            .register()
    }
}