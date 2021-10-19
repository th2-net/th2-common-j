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

package com.exactpro.th2.common.schema.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.util.JsonFormat;

public class ProtobufUtils {

    public static void changeRecursionLimit(int value) throws NoSuchFieldException, IllegalAccessException {
        changeCodedInputStreamLimit(value);
        changeJsonFormatLimit(value);
    }

    public static void changeCodedInputStreamLimit(int value) throws NoSuchFieldException, IllegalAccessException {
        modifyPrivateInt(CodedInputStream.class, "defaultRecursionLimit", value);
    }

    public static void changeJsonFormatLimit(int value) throws NoSuchFieldException, IllegalAccessException {
        modifyPrivateInt(JsonFormat.Parser.class, "DEFAULT_RECURSION_LIMIT", value);
    }

    private static void modifyPrivateInt(Class<?> clazz, String fieldName, int value) throws NoSuchFieldException, IllegalAccessException {
        Field recursionLimitField = clazz.getDeclaredField(fieldName);
        recursionLimitField.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(recursionLimitField, recursionLimitField.getModifiers() & ~Modifier.FINAL);
        recursionLimitField.set(null, value);
        recursionLimitField.setAccessible(false);
    }

}
