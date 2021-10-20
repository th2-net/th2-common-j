/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.common.tmp;

import java.time.Instant;

public interface RawMetadataBuilder {
    RawMetadataBuilder setSessionAlias(String alias);

    RawMetadataBuilder setDirection(Direction direction);

    RawMetadataBuilder setSequence(long sequence);

    RawMetadataBuilder addSubsequence(int subSequence);

    RawMetadataBuilder setTimestamp(Instant timestamp);

    RawMetadataBuilder putProperty(String key, String value);

    RawMetadataBuilder setProtocol(String protocol);
}
