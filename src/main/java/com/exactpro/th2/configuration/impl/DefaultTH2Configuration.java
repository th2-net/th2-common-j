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
package com.exactpro.th2.configuration.impl;

import static com.exactpro.th2.ConfigurationUtils.getJsonEnv;
import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.exactpro.th2.ConfigurationUtils;
import com.exactpro.th2.configuration.ITH2Configuration;
import com.exactpro.th2.configuration.QueueNames;
import com.fasterxml.jackson.core.type.TypeReference;

public class DefaultTH2Configuration implements ITH2Configuration {

    private Map<String, QueueNames> connectivityQueues = defaultIfNull(getJsonEnv("TH2_CONNECTIVITY_QUEUE_NAMES", new TypeReference<>() {}), emptyMap());

    @Override
    public Map<String, QueueNames> getConnectivityQueues() {
        return connectivityQueues;
    }

    public static DefaultTH2Configuration load(InputStream inputStream) throws IOException {
        return ConfigurationUtils.load(DefaultTH2Configuration.class, inputStream);
    }
}
