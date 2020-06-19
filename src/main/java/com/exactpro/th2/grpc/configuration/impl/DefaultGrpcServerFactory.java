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
package com.exactpro.th2.grpc.configuration.impl;

import java.net.InetSocketAddress;

import org.jetbrains.annotations.NotNull;

import com.exactpro.th2.grpc.configuration.IGrpcConfiguration;
import com.exactpro.th2.grpc.configuration.IGrpcServerFactory;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

public class DefaultGrpcServerFactory implements IGrpcServerFactory {

    private String host;
    private int port;

    public void init(@NotNull IGrpcConfiguration configuration) {
        this.host = configuration.getHost();
        this.port = configuration.getPort();
    }

    @Override
    public Server startServer(BindableService... services) {
        NettyServerBuilder builder = host == null ?
                NettyServerBuilder.forPort(port) :
                NettyServerBuilder.forAddress(InetSocketAddress.createUnresolved(host, port));
        for (BindableService service : services) {
            builder.addService(service);
        }
        return builder.build();
    }
}
