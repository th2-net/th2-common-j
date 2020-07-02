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

@Data
@Builder
public class AnnotationDescriptor {

    private String name;

    private String value;

    @Builder.Default
    private String packageName = "";


    public String getFullName() {
        return packageName + "." + name;
    }


    public static AnnotationDescriptor newInstance(AnnotationDescriptor ad) {
        return AnnotationDescriptor.builder()
                .name(ad.getName())
                .value(ad.getValue())
                .packageName(ad.getPackageName())
                .build();
    }

}
