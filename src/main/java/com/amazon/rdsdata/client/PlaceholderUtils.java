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

import lombok.Value;
import lombok.val;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

public class PlaceholderUtils {
    static String ERROR_NUMBER_OF_PARAMS_MISMATCH = "Number of placeholders does not match number of parameters";

    private static Pattern REGEX_NAMED_PLACEHOLDER = Pattern.compile(":([a-zA-Z0-9_]+)");

    // TODO: skip string literals and comments
    public static PlaceholderConvertResult convertToNamed(String sql, Object... parameters) {
        val parts = sql.split("\\?");
        checkArgument(numberOfParametersMatches(parts, parameters), ERROR_NUMBER_OF_PARAMS_MISMATCH);

        val resultingSql = new StringBuilder(parts[0]);
        val parametersMap = new HashMap<String, Object>();
        for (int i = 1; i < parts.length; i++) {
            resultingSql.append(":").append(i);
            resultingSql.append(parts[i]);
            parametersMap.put(String.valueOf(i), parameters[i - 1]);
        }

        return new PlaceholderConvertResult(resultingSql.toString(), parametersMap);
    }

    private static boolean numberOfParametersMatches(String[] parts, Object[] parameters) {
        return parts.length - 1 == parameters.length;
    }

    @Value
    public static class PlaceholderConvertResult {
        public final String sql;
        public final Map<String, Object> parameters;
    }

    // TODO: skip string literals and comments
    public static Set<String> findAll(String sql) {
        val matcher = REGEX_NAMED_PLACEHOLDER.matcher(sql);
        val result = new HashSet<String>();
        while (matcher.find()) {
            result.add(matcher.group(1));
        }
        return result;
    }
}
