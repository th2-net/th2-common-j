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
package com.exactpro.th2.proto.service.generator.antlr;

import com.exactpro.th2.proto.service.generator.antlr.descriptor.ServiceDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class ServiceClassGenerator {

    private static final Logger logger = LoggerFactory.getLogger(ProtoServiceParser.class);


    public static final String PACKAGE_ALIAS = "package";

    public static final String IMPORT_ALIAS = "import";

    public static final String PUBLIC_ALIAS = "public";

    public static final String INTERFACE_ALIAS = "interface";

    public static final String REQUEST_VAR_ALIAS = "request";

    public static final String SERVICE_SUFFIX = "Service";

    public static final String SERVICE_EXTENSION = ".java";

    public static final String LINE_SEPARATOR = System.lineSeparator();


    public static void generate(Path genDir, List<ServiceDescriptor> serviceDescriptors) throws IOException {

        for (var desc : serviceDescriptors) {

            var pathToClass = getPathToClass(genDir, desc.getPackageName()).toFile();

            if(!pathToClass.exists() && !pathToClass.mkdirs()){
                throw new IOException("Failed to create directories for class generation");
            }

            var serviceFileName = desc.getServiceName() + SERVICE_SUFFIX + SERVICE_EXTENSION;

            var serviceClassFile = Paths.get(pathToClass.toString(), serviceFileName);

            Files.write(serviceClassFile, descriptorToInterface(desc).getBytes(), CREATE, TRUNCATE_EXISTING);

        }

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
            addImportIfNotExist(method.getRequestType().getFullName(), data);
            addImportIfNotExist(method.getResponseType().getFullName(), data);
        }
    }

    private static void service(ServiceDescriptor serviceDescriptor, StringBuilder data) {
        data.append(LINE_SEPARATOR)
                .append(PUBLIC_ALIAS).append(" ")
                .append(INTERFACE_ALIAS).append(" ")
                .append(serviceDescriptor.getServiceName())
                .append(SERVICE_SUFFIX).append(" {")
                .append(LINE_SEPARATOR)
                .append(LINE_SEPARATOR);

        for (var method : serviceDescriptor.getMethods()) {
            data.append("    ")
                    .append(method.getResponseType().getName()).append(" ")
                    .append(method.getName()).append("(")
                    .append(method.getRequestType().getName()).append(" ")
                    .append(REQUEST_VAR_ALIAS).append(");")
                    .append(LINE_SEPARATOR);
        }

        data.append(LINE_SEPARATOR).append("}");
    }

    private static void addImportIfNotExist(String packageName, StringBuilder data) {
        if (!data.toString().contains(packageName)) {
            data.append(IMPORT_ALIAS).append(" ")
                    .append(packageName).append(";")
                    .append(LINE_SEPARATOR);
        }
    }

    private static Path getPathToClass(Path genDir, String packageName) {
        return Paths.get(genDir.toString(), packageName.split("\\."));
    }

}
