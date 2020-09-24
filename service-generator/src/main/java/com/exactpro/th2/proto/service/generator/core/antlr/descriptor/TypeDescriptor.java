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

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Objects;

@Data
@Builder
@EqualsAndHashCode(callSuper = true)
public class TypeDescriptor extends AbstractTypeableDescriptor {

    private String name;

    @Builder.Default
    private String packageName = "";

    private TypeDescriptor genericType;


    public String getNameAsParam() {
        if (isParameterized()) {
            return name + "<" + genericType.getName() + ">";
        }
        return name;
    }

    public boolean isParameterized() {
        return Objects.nonNull(genericType);
    }


    public static TypeDescriptor newInstance(TypeDescriptor td) {
        TypeDescriptor genericType = null;
        if (Objects.nonNull(td.getGenericType())) {
            genericType = newInstance(td.getGenericType());
        }

        return TypeDescriptor.builder()
                .name(td.getName())
                .packageName(td.getPackageName())
                .genericType(genericType)
                .build();
    }

}
