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

import com.exactpro.th2.common.message.configuration.MessageRouterConfiguration;
import com.exactpro.th2.common.strategy.routeStrategy.RoutingStrategy;
import com.exactpro.th2.common.strategy.routeStrategy.json.JsonDeserializerRoutingStategy;
import com.exactpro.th2.grpc.configuration.GrpcRouterConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class TestMqConfiguration {

    private static final String pathToMQConfiguration = "./src/test/resources/com.exactpro.th2/grpc/configuration/test/mq.json";
    private static final String pathToGRPCConfiguration = "./src/test/resources/com.exactpro.th2/grpc/configuration/test/grpc.json";

    @Test
    public void testMqConfiguration() {
        try {
            MessageRouterConfiguration conf = new ObjectMapper().readerFor(MessageRouterConfiguration.class).readValue(new File(pathToMQConfiguration));

            Assert.assertEquals(Collections.singleton("fix_in"), conf.getQueuesAliasByAttribute("fix", "in"));
        } catch (IOException e) {
            Assert.fail();
        }
    }

    @Test
    public void testGrpcConfiguration() {
        //TODO: Add test

//        try {
//            var mapper = new ObjectMapper();
//
//            SimpleModule module = new SimpleModule();
//            module.addDeserializer(RoutingStrategy.class, new JsonDeserializerRoutingStategy());
//            mapper.registerModule(module);
//
//            var conf = mapper.readerFor(GrpcRouterConfiguration.class).readValue(new File(pathToGRPCConfiguration));
//
//        } catch (IOException e) {
//            Assert.fail();
//        }

    }

}
