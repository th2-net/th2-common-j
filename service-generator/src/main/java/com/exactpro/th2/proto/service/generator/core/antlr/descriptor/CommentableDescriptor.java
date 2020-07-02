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
package com.exactpro.th2.proto.service.generator.core.antlr.descriptor;

import lombok.Data;
import lombok.experimental.SuperBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


@Data
@SuperBuilder(toBuilder = true)
public abstract class CommentableDescriptor {

    protected List<String> comments;


    public String getCommentsAsJavaDoc(String indent) {
        if (comments.isEmpty()) {
            return "";
        }

        return "\n" + comments.stream()
                .flatMap(c -> Arrays.stream(c.split("\n")))
                .map(c -> indent + " * " + c)
                .collect(Collectors.joining(
                        System.lineSeparator(),
                        indent + "/**\n",
                        "\n" + indent + " */"
                ));
    }

}
