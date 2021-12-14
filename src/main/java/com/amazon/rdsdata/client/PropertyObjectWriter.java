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

import static com.amazon.rdsdata.client.FieldPropertyWriter.fieldPropertyWriterFor;
import static com.amazon.rdsdata.client.SetterPropertyWriter.setterPropertyWriterFor;

@RequiredArgsConstructor
class PropertyObjectWriter<T> extends ObjectWriter<T> {
    private final Class<T> mapperClass;
    private final List<String> fieldNames;
    private final MappingOptions mappingOptions;

    public static <T> ObjectWriter<T> create(Class<T> mapperClass, List<String> fieldNames, MappingOptions mappingOptions) {
        return new PropertyObjectWriter<>(mapperClass, fieldNames, mappingOptions);
    }

    @Override
    public T write(ExecutionResult.Row row) {
        val constructor = findNoArgsConstructor()
                .orElseThrow(() -> MappingException.cannotCreateInstanceViaNoArgsConstructor(mapperClass));
        val instance = createInstance(constructor);
        setAllProperties(instance, row);
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

    private void setAllProperties(T instance, ExecutionResult.Row row) {
        for (int i = 0; i < fieldNames.size(); i++) {
            val name = fieldNames.get(i);
            val index = i;
            findPropertyWriter(instance, name)
                .ifPresent(field -> setProperty(field, row, index));
        }
    }

    private void setProperty(PropertyWriter propertyWriter, ExecutionResult.Row row, int index) {
        val value = row.getValue(index, propertyWriter.getType());
        propertyWriter.write(value);
    }

    private Optional<PropertyWriter> findPropertyWriter(Object instance, String fieldName) {
        val result = setterPropertyWriterFor(instance, fieldName)
            .map(Optional::of)
            .orElseGet(() -> fieldPropertyWriterFor(instance, fieldName));

        if (!result.isPresent() && !mappingOptions.ignoreMissingSetters) {
            throw MappingException.noFieldOrSetter(instance.getClass(), fieldName);
        }
        return result;
    }
}
