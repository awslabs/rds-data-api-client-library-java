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

import static com.amazon.rdsdata.client.FieldWriter.fieldWriterFor;
import static com.amazon.rdsdata.client.SetterWriter.setterWriterFor;

public abstract class Writer {
    public static Writer forName(Object instance, String fieldName) {
        return setterWriterFor(instance, fieldName)
                .orElseGet(() -> fieldWriterFor(instance, fieldName)
                .orElseThrow(() -> MappingException.noFieldOrSetter(instance.getClass(), fieldName)));
    }

    public abstract void setValue(Object value);
    public abstract Class<?> getType();
}
