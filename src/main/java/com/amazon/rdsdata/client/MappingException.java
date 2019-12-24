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

import com.amazonaws.services.rdsdata.model.Field;
import lombok.val;

public class MappingException extends RuntimeException {
    static final String ERROR_NO_FIELD_OR_SETTER = "Class '%s' does not contain field '%s' or a corresponding setter";
    static final String ERROR_CANNOT_ACCESS_FIELD = "Cannot access field '%s' in class %s";
    static final String ERROR_CANNOT_CREATE_INSTANCE = "Cannot create instance of type %s";
    static final String ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS = "Cannot create instance of type %s: public no args constructor not found";
    static final String ERROR_STATIC_FIELD = "Field '%s' in class %s is static";
    static final String ERROR_CANNOT_SET_VALUE = "Cannot set value '%s'";
    static final String ERROR_EMPTY_RESULT_SET = "Result set is empty";
    static final String ERROR_CANNOT_CONVERT_TO_TYPE = "Cannot convert field %s to type %s";

    private MappingException(String message) {
        super(message);
    }

    public MappingException(String message, Throwable cause) {
        super(message, cause);
    }

    public static MappingException noFieldOrSetter(Class<?> clazz, String fieldName) {
        val message = String.format(ERROR_NO_FIELD_OR_SETTER, clazz.getName(), fieldName);
        return new MappingException(message);
    }

    public static MappingException cannotAccessField(Class<?> clazz, String fieldName) {
        val message = String.format(ERROR_CANNOT_ACCESS_FIELD, fieldName, clazz.getName());
        return new MappingException(message);
    }

    public static MappingException cannotCreateInstanceViaNoArgsConstructor(Class<?> clazz) {
        val message = String.format(ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS, clazz.getName());
        return new MappingException(message);
    }

    public static MappingException cannotCreateInstance(Class<?> clazz, Throwable cause) {
        val message = String.format(ERROR_CANNOT_CREATE_INSTANCE, clazz.getName());
        return new MappingException(message, cause);
    }

    public static MappingException staticField(Class<?> clazz, String fieldName) {
        val message = String.format(ERROR_STATIC_FIELD, fieldName, clazz.getName());
        return new MappingException(message);
    }

    public static MappingException cannotSetValue(String fieldName, Throwable cause) {
        val message = String.format(ERROR_CANNOT_SET_VALUE, fieldName);
        return new MappingException(message, cause);
    }

    public static MappingException emptyResultSet() {
        return new MappingException(ERROR_EMPTY_RESULT_SET);
    }

    public static MappingException cannotConvertToType(Field field, Class targetType) {
        val message = String.format(ERROR_CANNOT_CONVERT_TO_TYPE, field.toString(), targetType.toString());
        return new MappingException(message);
    }
}
