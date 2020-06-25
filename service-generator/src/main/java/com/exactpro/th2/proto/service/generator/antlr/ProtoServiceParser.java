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

import com.exactpro.th2.proto.service.generator.antlr.descriptor.MethodDescriptor;
import com.exactpro.th2.proto.service.generator.antlr.descriptor.ServiceDescriptor;
import com.exactpro.th2.proto.service.generator.antlr.descriptor.TypeDescriptor;
import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class ProtoServiceParser {

    private static final Logger logger = LoggerFactory.getLogger(ProtoServiceParser.class);


    private static final String PROTO_EXTENSION = ".proto";

    private static final String PROTO_MSG_ALIAS = "message";

    private static final String PROTO_SERVICE_ALIAS = "service";

    private static final String PROTO_OPTION_ALIAS = "option";

    private static final String PROTO_PACKAGE_ALIAS = "java_package";


    public static List<ServiceDescriptor> getServiceDescriptors(Path protoDir) throws IOException {

        var protoFiles = Files.walk(protoDir)
                .filter(path -> Files.isRegularFile(path) && path.getFileName().toString().endsWith(PROTO_EXTENSION))
                .collect(Collectors.toList());

        Map<String, String> messageToPackage = new HashMap<>();
        List<ServiceDescriptor> serviceDescriptors = new ArrayList<>();

        for (var protoFile : protoFiles) {

            Protobuf3Lexer lexer = new Protobuf3Lexer(new ANTLRFileStream(protoFile.toString()));
            Protobuf3Parser parser = new Protobuf3Parser(new CommonTokenStream(lexer));
            Protobuf3Parser.ProtoContext protoTree = parser.proto();

            // StringBuilder for apply reference behavior
            var packageName = new StringBuilder();

            for (var child : protoTree.children) {

                extractPackage(child, packageName);

                extractService(child, packageName.toString(), serviceDescriptors);

                extractMessage(child, packageName.toString(), messageToPackage);

            }

        }

        setupMsgPackages(serviceDescriptors, messageToPackage);

        if (serviceDescriptors.isEmpty()) {
            logger.warn("No services was found in provided proto files");
        }

        return serviceDescriptors;
    }


    private static void setupMsgPackages(List<ServiceDescriptor> serviceDesc, Map<String, String> messageToPackage) {
        for (var sDesc : serviceDesc) {
            for (var method : sDesc.getMethods()) {
                var reqType = method.getRequestType();
                var respType = method.getResponseType();

                var reqTypeName = reqType.getName();
                var respTypeName = respType.getName();

                var reqPackageName = messageToPackage.get(reqTypeName);
                var respPackageName = messageToPackage.get(respTypeName);

                checkExistence(reqTypeName, reqPackageName);
                checkExistence(respTypeName, respPackageName);

                reqType.setPackageName(reqPackageName);
                respType.setPackageName(respPackageName);
            }
        }
    }

    private static void checkExistence(String msgName, String packageName) {
        if (Objects.isNull(packageName)) {
            throw new IllegalStateException(String.format("Message<%s> definition " +
                    "not found in provided proto files", msgName));
        }
    }

    private static void extractPackage(ParseTree node, StringBuilder currentPackage) {
        if (node.getChildCount() > 3) {
            var option = node.getChild(0).getText();
            var optionName = node.getChild(1).getText();
            var value = node.getChild(3).getText();

            if (option.equals(PROTO_OPTION_ALIAS)
                    && optionName.equals(PROTO_PACKAGE_ALIAS)
                    && currentPackage.toString().isEmpty()) {
                currentPackage.append(value.replace("\"", "").replace("'", ""));
            }
        }
    }

    private static void extractService(ParseTree node, String packageName, List<ServiceDescriptor> serviceDescs) {
        extractEntity(node, PROTO_SERVICE_ALIAS, (serviceName, entityNode) -> {

            var serviceDesc = new ServiceDescriptor();

            serviceDesc.setServiceName(serviceName);
            serviceDesc.setPackageName(packageName);
            serviceDesc.setMethods(getMethodDescriptors(entityNode));

            serviceDescs.add(serviceDesc);
        });
    }

    private static void extractMessage(ParseTree node, String packageName, Map<String, String> messageToPackage) {
        extractEntity(node, PROTO_MSG_ALIAS, (msgName, entityNode) -> messageToPackage.put(msgName, packageName));
    }

    private static void extractEntity(ParseTree node, String targetEntity, BiConsumer<String, ParseTree> entityConsumer) {
        if (node.getChildCount() > 0) {

            var potentialEntity = node.getChild(0);

            if (potentialEntity.getChildCount() > 0) {

                var option = potentialEntity.getChild(0).getText();

                if (option.equals(targetEntity)) {
                    var entityName = potentialEntity.getChild(1).getText();
                    entityConsumer.accept(entityName, potentialEntity);
                }
            }
        }
    }

    private static List<MethodDescriptor> getMethodDescriptors(ParseTree serviceNode) {

        var startRpcDeclarationIndex = 3;
        var endRpcDeclarationIndex = serviceNode.getChildCount() - 1;

        var methodNameIndex = 1;
        var methodRequestTypeIndex = 3;
        var methodResponseTypeIndex = 7;

        List<MethodDescriptor> methodDescriptors = new ArrayList<>();

        for (int i = startRpcDeclarationIndex; i < endRpcDeclarationIndex; i++) {
            var methodNode = serviceNode.getChild(i);

            var methodName = methodNode.getChild(methodNameIndex).getText();
            var methodRequestType = methodNode.getChild(methodRequestTypeIndex).getText();
            var methodResponseType = methodNode.getChild(methodResponseTypeIndex).getText();

            var rqTypeDesc = new TypeDescriptor();
            rqTypeDesc.setName(methodRequestType);

            var respTypeDesc = new TypeDescriptor();
            respTypeDesc.setName(methodResponseType);

            var methodDesc = new MethodDescriptor();
            methodDesc.setName(methodName);
            methodDesc.setRequestType(rqTypeDesc);
            methodDesc.setResponseType(respTypeDesc);

            methodDescriptors.add(methodDesc);
        }

        return methodDescriptors;
    }

}
