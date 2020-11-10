/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.schema.message.impl.rabbitmq.configuration;

import lombok.Data;

@Data
public class ResendMessageConfiguration {

    private int resendWorkers = Runtime.getRuntime().availableProcessors() * 4;

    private long minDelay = 200;

    private long maxDelay = 5000;

    private int countRejectsToBlockSender = 1;

    public void setResendWorkers(int resendWorkers) {
        if (resendWorkers > 0) {
            this.resendWorkers = resendWorkers;
        }
    }

    public void setMinDelay(long minDelay) {
        if (minDelay > -1) {
            this.minDelay = minDelay;
        }
    }

    public void setCountRejectsToBlockSender(int countRejectsToBlockSender) {
        if (countRejectsToBlockSender > 0) {
            this.countRejectsToBlockSender = countRejectsToBlockSender;
        }
    }
}
