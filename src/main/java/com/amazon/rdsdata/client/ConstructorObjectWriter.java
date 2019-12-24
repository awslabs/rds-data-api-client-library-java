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

import lombok.val;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class ConstructorObjectWriter<T> extends ObjectWriter<T> {
    private final Constructor<T> constructor;
    private final Map<String, Integer> indexByName;

    ConstructorObjectWriter(Constructor<T> constructor, List<String> fieldNames) {
        this.constructor = constructor;
        this.indexByName = buildIndexByNameMap(fieldNames);
    }

    private static Map<String, Integer> buildIndexByNameMap(List<String> fieldNames) {
        return IntStream.range(0, fieldNames.size())
                .boxed()
                .collect(toMap(fieldNames::get, i -> i));
    }

    // Tries to create an ObjectWriter that populates object via all-args constructor
    public static <T> Optional<ObjectWriter<T>> create(Class<T> mapperClass, List<String> fieldNames) {
        if (fieldNames.size() == 0) {
            return Optional.empty();
        }

        return Stream.of(mapperClass.getDeclaredConstructors())
                .filter(c -> containsAllFields(c, fieldNames))
                // TODO: check if public
                .findFirst()
                .map(c -> new ConstructorObjectWriter<>((Constructor<T>) c, fieldNames));
    }

    private static boolean containsAllFields(Constructor<?> constructor, List<String> fieldNames) {
        val parameterNames = getParameterNames(constructor);
        return parameterNames.equals(new HashSet<>(fieldNames));
    }

    private static Set<String> getParameterNames(Constructor<?> constructor) {
        return Arrays.stream(constructor.getParameters())
                .map(Parameter::getName)
                .collect(toSet());
    }

    @Override
    public T write(ExecutionResult.Row row) {
        try {
            return constructor.newInstance(buildArgumentsList(row));
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
            throw MappingException.cannotCreateInstance(constructor.getDeclaringClass(), e);
        }
    }

    private Object[] buildArgumentsList(ExecutionResult.Row row) {
        val parameters = constructor.getParameters();
        val args = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            val name = parameters[i].getName();
            val indexInFieldsList = indexByName.get(name);
            val value = row.getValue(indexInFieldsList, parameters[i].getType());
            args[i] = value;
        }
        return args;
    }
}
