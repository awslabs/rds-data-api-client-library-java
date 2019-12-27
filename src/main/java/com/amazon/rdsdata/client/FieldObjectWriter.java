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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
class FieldObjectWriter<T> extends ObjectWriter<T> {
    private final Class<T> mapperClass;
    private final List<String> fieldNames;

    public static <T> ObjectWriter<T> create(Class<T> mapperClass, List<String> fieldNames) {
        return new FieldObjectWriter<>(mapperClass, fieldNames);
    }

    @Override
    public T write(ExecutionResult.Row row) {
        val constructor = findNoArgsConstructor()
                .orElseThrow(() -> MappingException.cannotCreateInstanceViaNoArgsConstructor(mapperClass));
        val instance = createInstance(constructor);
        populate(instance, row);
        return instance;
    }

    private Optional<Constructor<T>> findNoArgsConstructor() {
        try {
            return Optional.of(mapperClass.getDeclaredConstructor());
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }

    private T createInstance(Constructor<T> constructor) {
        try {
            return constructor.newInstance();
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
            throw MappingException.cannotCreateInstance(constructor.getDeclaringClass(), e);
        }
    }

    private void populate(T instance, ExecutionResult.Row row) {
        for (int i = 0; i < fieldNames.size(); i++) {
            val name = fieldNames.get(i);
            val field = Writer.forName(instance, name);
            val value = row.getValue(i, field.getType());

            field.setValue(value);
        }
    }
}
