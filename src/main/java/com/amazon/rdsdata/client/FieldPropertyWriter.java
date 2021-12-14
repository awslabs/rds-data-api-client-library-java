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

import lombok.AllArgsConstructor;
import lombok.val;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;

import static lombok.AccessLevel.PRIVATE;

@AllArgsConstructor(access = PRIVATE)
class FieldPropertyWriter implements PropertyWriter {
    private Object instance;
    private Class<?> fieldType;
    private Field field;

    static Optional<PropertyWriter> fieldPropertyWriterFor(Object instance, String fieldName) {
        val instanceType = instance.getClass();
        try {
            val field = instanceType.getDeclaredField(fieldName);
            if (Modifier.isStatic(field.getModifiers())) {
                throw MappingException.staticField(instanceType, fieldName);
            }

            val writer = new FieldPropertyWriter(instance, field.getType(), field);
            return Optional.of(writer);
        } catch (NoSuchFieldException e) {
            return Optional.empty();
        }
    }

    @Override
    public void write(Object value) {
        try {
            field.set(instance, value);
        } catch (IllegalAccessException e) {
            throw MappingException.cannotAccessField(instance.getClass(), field.getName());
        }
    }

    @Override
    public Class<?> getType() {
        return fieldType;
    }
}
