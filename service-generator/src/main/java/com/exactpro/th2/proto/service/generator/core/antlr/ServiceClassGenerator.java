/*****************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/

package com.exactpro.th2.proto.service.generator.core.antlr;

import com.exactpro.th2.proto.service.generator.core.antlr.descriptor.ServiceDescriptor;
import com.exactpro.th2.proto.service.generator.core.antlr.descriptor.TypeDescriptor;
import com.exactpro.th2.proto.service.generator.core.antlr.descriptor.TypeableDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class ServiceClassGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ServiceClassGenerator.class);


    public static final String PACKAGE_ALIAS = "package";

    public static final String IMPORT_ALIAS = "import";

    public static final String PUBLIC_ALIAS = "public";

    public static final String INTERFACE_ALIAS = "interface";

    public static final String REQUEST_VAR_ALIAS = "arg";

    public static final String SERVICE_SUFFIX = "Service";

    public static final String SERVICE_EXTENSION = ".java";

    public static final String SERVICE_INDENT = "    ";

    public static final String LINE_SEPARATOR = System.lineSeparator();


    public static void generate(Path genDir, List<ServiceDescriptor> serviceDescriptors) throws IOException {
        Objects.requireNonNull(genDir, "Target directory path for generated source cannot be null!");
        Objects.requireNonNull(serviceDescriptors, "Service descriptors collection cannot be null!");

        for (var desc : serviceDescriptors) {
            logger.info("Creating java interface of '{}' services", desc.getName());

            var pathToClass = getPathToClass(genDir, desc.getPackageName()).toFile();

            if (!pathToClass.exists() && !pathToClass.mkdirs()) {
                throw new IOException("Failed to create directories for interface generation");
            }

            var serviceFileName = desc.getName() + SERVICE_SUFFIX + SERVICE_EXTENSION;

            var serviceClassFile = Paths.get(pathToClass.toString(), serviceFileName);

            Files.write(serviceClassFile, descriptorToInterface(desc).getBytes(), CREATE, TRUNCATE_EXISTING);

        }

        logger.info("{} interface(s) successfully created", serviceDescriptors.size());

    }


    private static String descriptorToInterface(ServiceDescriptor serviceDescriptor) {

        var data = new StringBuilder();

        packageName(serviceDescriptor, data);

        imports(serviceDescriptor, data);

        service(serviceDescriptor, data);

        return data.toString();
    }

    private static void packageName(ServiceDescriptor serviceDescriptor, StringBuilder data) {
        data.append(PACKAGE_ALIAS).append(" ")
                .append(serviceDescriptor.getPackageName()).append(";")
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR);
    }

    private static void imports(ServiceDescriptor serviceDescriptor, StringBuilder data) {

        for (var method : serviceDescriptor.getMethods()) {
            for (var reqType : method.getRequestTypes()) {
                importsRec(reqType, data);
            }
            importsRec(method.getResponseType(), data);
        }

        for (var ann : serviceDescriptor.getAnnotations()) {
            addImportIfNotExist(ann, data);
        }

    }

    private static void importsRec(TypeDescriptor typeDescriptor, StringBuilder data) {
        addImportIfNotExist(typeDescriptor, data);
        if (typeDescriptor.isParameterized()) {
            importsRec(typeDescriptor.getGenericType(), data);
        }
    }

    private static void service(ServiceDescriptor sd, StringBuilder data) {

        data.append(sd.getCommentsAsJavaDoc("")).append(LINE_SEPARATOR);

        for (var ann : sd.getAnnotations()) {
            data.append("@").append(ann.getName()).append("(")
                    .append(ann.getValue()).append(")")
                    .append(LINE_SEPARATOR);
        }

        data.append(PUBLIC_ALIAS).append(" ")
                .append(INTERFACE_ALIAS).append(" ")
                .append(sd.getName())
                .append(SERVICE_SUFFIX).append(" {")
                .append(LINE_SEPARATOR);


        for (var method : sd.getMethods()) {

            String methodName = Character.toLowerCase(method.getName().charAt(0)) + method.getName().substring(1);

            data.append(method.getCommentsAsJavaDoc(SERVICE_INDENT))
                    .append(LINE_SEPARATOR)
                    .append(SERVICE_INDENT)
                    .append(method.getResponseType().getNameAsParam()).append(" ")
                    .append(methodName).append("(");

            var reqTypes = method.getRequestTypes();

            int argCounter = 1;
            for (int i = 0; i < reqTypes.size(); i++) {
                var reqType = reqTypes.get(i);
                data.append(reqType.getNameAsParam()).append(" ")
                        .append(REQUEST_VAR_ALIAS).append(argCounter++)
                        .append(i < reqTypes.size() - 1 ? ", " : "");
            }

            data.append(");").append(LINE_SEPARATOR);
        }

        data.append(LINE_SEPARATOR).append("}");
    }

    private static void addImportIfNotExist(TypeableDescriptor typeableDescriptor, StringBuilder data) {
        var fullName = typeableDescriptor.getFullName();
        var packageName = typeableDescriptor.getPackageName();
        if (Objects.nonNull(packageName) && !packageName.isEmpty() && !data.toString().contains(fullName) ) {
            data.append(IMPORT_ALIAS).append(" ")
                    .append(fullName).append(";")
                    .append(LINE_SEPARATOR);
        }
    }

    private static Path getPathToClass(Path genDir, String packageName) {
        return Paths.get(genDir.toString(), packageName.split("\\."));
    }

}
