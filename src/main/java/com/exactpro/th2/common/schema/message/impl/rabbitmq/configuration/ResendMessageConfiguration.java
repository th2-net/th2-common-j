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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResendMessageConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResendMessageConfiguration.class);

    private int maxResendWorkers = Runtime.getRuntime().availableProcessors();

    private long minDelay = 200;

    private long maxDelay = 5000;

    private int countRejectsToBlockSender = 1;

    public int getMaxResendWorkers() {
        return maxResendWorkers;
    }

    public void setMaxResendWorkers(int maxResendWorkers) {
        if (maxResendWorkers > 0) {
            this.maxResendWorkers = maxResendWorkers;
        }
    }

    public long getMinDelay() {
        return minDelay;
    }

    public long getMaxDelay() {
        return maxDelay;
    }

    public void setMaxDelay(long maxDelay) {
        this.maxDelay = maxDelay;
    }

    public int getCountRejectsToBlockSender() {
        return countRejectsToBlockSender;
    }

    public void setMinDelay(long minDelay) {
        if (minDelay > -1) {
            this.minDelay = minDelay;
        } else {
            LOGGER.warn("Can not set property 'minDelay', because it is must be more than -1, but current value = {}", minDelay);
        }
    }

    public void setCountRejectsToBlockSender(int countRejectsToBlockSender) {
        if (countRejectsToBlockSender > 0) {
            this.countRejectsToBlockSender = countRejectsToBlockSender;
        } else {
            LOGGER.warn("Can not set property 'countRejectsToBlockSender', because it is must be more than 0, but current value = {}", countRejectsToBlockSender);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("maxResendWorkers", maxResendWorkers)
                .append("minDelay", minDelay)
                .append("maxDelay", maxDelay)
                .append("countRejectsToBlockSender", countRejectsToBlockSender)
                .toString();
    }
}
