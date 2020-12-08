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
package com.exactpro.th2.common.schema.grpc.configuration;

import com.exactpro.th2.service.RetryPolicy;

public class GrpcRetryConfiguration implements RetryPolicy {

    private int maxRetriesAttempts = 5;

    private int minMethodRetriesTimeout = 100;

    private int maxMethodRetriesTimeout = 2_000;

    @Override
    public long getDelay(int currentAttempt) {
        return minMethodRetriesTimeout + (maxRetriesAttempts > 1 ? (maxMethodRetriesTimeout - minMethodRetriesTimeout) / (maxRetriesAttempts - 1) * currentAttempt : 0);
    }

    @Override
    public int getMaxAttempts() {
        return maxRetriesAttempts;
    }

    public void setMaxRetriesAttempts(int maxRetriesAttempts) {
        this.maxRetriesAttempts = maxRetriesAttempts;
    }

    public int getMinMethodRetriesTimeout() {
        return minMethodRetriesTimeout;
    }

    public void setMinMethodRetriesTimeout(int minMethodRetriesTimeout) {
        this.minMethodRetriesTimeout = minMethodRetriesTimeout;
    }

    public int getMaxMethodRetriesTimeout() {
        return maxMethodRetriesTimeout;
    }

    public void setMaxMethodRetriesTimeout(int maxMethodRetriesTimeout) {
        this.maxMethodRetriesTimeout = maxMethodRetriesTimeout;
    }
}