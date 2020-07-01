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
package com.exactpro.th2.grpc.configuration.test;

import java.nio.file.Path;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import com.exactpro.th2.common.factory.AbstractCommonFactory;
import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.grpc.configuration.GrpcRouterConfiguration;

public class TestConfiguration extends AbstractCommonFactory{

    @Override
    protected Path getPathToRabbitMQConfiguration() {
        return null;
    }

    @Override
    protected Path getPathToMessageRouterConfiguration() {
        return Path.of("./src/test/resources/com.exactpro.th2/grpc/configuration/test/mq.json");
    }

    @Override
    protected Path getPathToGrpcRouterConfiguration() {
        return Path.of("./src/test/resources/com.exactpro.th2/grpc/configuration/test/grpc.json");
    }

    @Override
    protected Path getPathToCustomConfiguration() {
        return null;
    }

    @Test
    public void testMqConfiguration() {
        MessageRouterConfiguration conf = this.getMessageRouterConfiguration();
        Assert.assertEquals(Collections.singleton("fix_in"), conf.getQueuesAliasByAttribute("fix", "in"));
    }

    @Test
    public void testGrpcConfiguration() {
        GrpcRouterConfiguration conf = this.getGrpcRouterConfiguration();
        var actStrategy = conf.getServices().get("ActService").getStrategy();
        Assert.assertEquals("name3", actStrategy.getEndpoint(null));
        Assert.assertEquals("name4", actStrategy.getEndpoint(null));
        Assert.assertEquals("name3", actStrategy.getEndpoint(null));
    }

}
