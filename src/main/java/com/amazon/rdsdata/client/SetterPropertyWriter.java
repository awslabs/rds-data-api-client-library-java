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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PRIVATE;

@AllArgsConstructor(access = PRIVATE)
class SetterPropertyWriter implements PropertyWriter {
    private Object instance;
    private Method setter;
    private String fieldName;

    static Optional<PropertyWriter> setterPropertyWriterFor(Object instance, String fieldName) {
        val instanceType = instance.getClass();
        val setterName = buildSetterName(fieldName);

        val possibleSetterMethods = Stream.of(instanceType.getMethods())
            .filter(method -> method.getName().equals(setterName))
            .filter(SetterPropertyWriter::isNotStatic)
            .filter(SetterPropertyWriter::hasOneParameter)
            .filter(SetterPropertyWriter::isPublic)
            .collect(toList());

        if (possibleSetterMethods.size() > 1) {
            throw MappingException.ambiguousSetter(fieldName, possibleSetterMethods);
        }

        if (possibleSetterMethods.size() == 0) {
            return Optional.empty();
        }

        return Optional.of(new SetterPropertyWriter(instance, possibleSetterMethods.get(0), fieldName));
    }

    private static boolean isNotStatic(Method method) {
        return !Modifier.isStatic(method.getModifiers());
    }

    private static boolean hasOneParameter(Method method) {
        return method.getParameterCount() == 1;
    }

    private static boolean isPublic(Method method) {
        return Modifier.isPublic(method.getModifiers());
    }

    private static String buildSetterName(String fieldName) {
        return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    @Override
    public void write(Object value) {
        try {
            setter.invoke(instance, value);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw MappingException.cannotSetValue(fieldName, e);
        }
    }

    @Override
    public Class<?> getType() {
        return setter.getParameterTypes()[0];
    }
}
