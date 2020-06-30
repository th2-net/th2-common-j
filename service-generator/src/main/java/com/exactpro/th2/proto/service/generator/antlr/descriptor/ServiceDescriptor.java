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
package com.exactpro.th2.proto.service.generator.antlr.descriptor;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class ServiceDescriptor extends CommentableDescriptor {

    private String name;

    @Builder.Default
    private String packageName = "";

    private List<MethodDescriptor> methods;


    public ServiceDescriptor toAsync() {

        var asd = copy();

        asd.getMethods().forEach(method ->
                method.getRequestTypes().add(TypeDescriptor.builder()
                        .name("StreamObserver")
                        .packageName("io.grpc.stub")
                        .genericType(method.getResponseType())
                        .build())
        );

        asd.setName("Async" + asd.getName());

        return asd;
    }

    public ServiceDescriptor copy() {

        var methods = this.methods.stream()
                .map(MethodDescriptor::copy)
                .collect(Collectors.toList());

        return ServiceDescriptor.builder()
                .comments(new ArrayList<>(this.comments))
                .name(this.name)
                .packageName(this.packageName)
                .methods(methods)
                .build();
    }

}
