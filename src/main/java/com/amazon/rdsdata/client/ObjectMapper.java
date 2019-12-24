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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ObjectMapper {
    private final Set<String> placeholders;

    public ObjectMapper(String sql) {
        placeholders = PlaceholderUtils.findAll(sql);
    }

    public Map<String, Object> map(Object o) {
        val fieldMapper = new FieldMapper(o);
        return placeholders.stream()
                .collect(Collectors.toMap(Function.identity(), fieldMapper::read));
    }
}
