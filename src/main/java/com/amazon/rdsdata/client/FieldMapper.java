/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazon.rdsdata.client;

import lombok.RequiredArgsConstructor;
import lombok.val;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

@RequiredArgsConstructor
class FieldMapper {
    static Object NULL = new Object();

    static String ERROR_FIELD_NOT_FOUND = "Cannot find field or getter corresponding to placeholder '%s' in object '%s'";
    static String ERROR_VOID_RETURN_TYPE_NOT_SUPPORTED = "Void return type is not supported";

    private final Object object;

    public Object read(String fieldName) {
        return getValueFromGetter(fieldName)
                .orElseGet(() -> getValueFromField(fieldName)
                        .orElseThrow(() -> buildCannotFindException(fieldName)));
    }

    private RuntimeException buildCannotFindException(String fieldName) {
        val errorMessage = String.format(ERROR_FIELD_NOT_FOUND, fieldName, object);
        return new IllegalArgumentException(errorMessage);
    }

    private Optional<Object> getValueFromField(String fieldName) {
        try {
            val field = getField(object, fieldName);
            field.setAccessible(true);
            val result = field.get(object);
            return Optional.of(result == null ? NULL : result);
        } catch (NoSuchFieldException e) {
            return Optional.empty();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access field " + fieldName + " from object " + object);
        }
    }

    private Field getField(Object object, String fieldName) throws NoSuchFieldException {
        try {
            return object.getClass().getField(fieldName);
        } catch (NoSuchFieldException e) {
            // Falling back to getDeclaredField() to access private fields
            return object.getClass().getDeclaredField(fieldName);
        }
    }

    private Optional<Object> getValueFromGetter(String fieldName) {
        val methodName = buildGetterName(fieldName);
        try {
            val method = getMethod(object, methodName);
            checkArgument(method.getReturnType() != void.class, ERROR_VOID_RETURN_TYPE_NOT_SUPPORTED);

            method.setAccessible(true);
            val result = method.invoke(object);
            return Optional.of(result == null ? NULL : result);
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Cannot access method " + methodName + " from object " + object);
        }
    }

    private Method getMethod(Object object, String methodName) throws NoSuchMethodException {
        try {
            return object.getClass().getMethod(methodName);
        } catch (NoSuchMethodException e) {
            // Falling back to getDeclaredMethod() to access private methods
            return object.getClass().getDeclaredMethod(methodName);
        }
    }

    private String buildGetterName(String fieldName) {
        return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }
}
