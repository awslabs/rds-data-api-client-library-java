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

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@RequiredArgsConstructor
public class Executor {
    private final String sql;
    private final RdsDataClient client;
    private List<Object> paramSets = emptyList();
    private String transactionId = ""; // RDS Data API understands empty string as "no transaction"

    public Executor withParameter(Object param) {
        this.paramSets = singletonList(param);
        return this;
    }

    public Executor withParamSets(List<Object> params) {
        this.paramSets = params;
        return this;
    }

    public Executor withParamSets(Object... params) {
        return withParamSets(asList(params));
    }

    public ExecutionResult execute() {
        return paramSets.size() > 1 ? executeAsBatch() : executeAsSingle();
    }

    private ExecutionResult executeAsBatch() {
        val paramSetsAsMaps = paramSets.stream()
                .map(paramSet -> toMap(sql, paramSet))
                .collect(toList());
        return client.batchExecuteStatement(transactionId, sql, paramSetsAsMaps);
    }

    private ExecutionResult executeAsSingle() {
        val firstParamSetAsMap = paramSets.stream()
                .findFirst()
                .map(paramSet -> toMap(sql, paramSet))
                .orElse(emptyMap());
        return client.executeStatement(transactionId, sql, firstParamSetAsMap);
    }

    private Map<String, Object> toMap(String sql, Object paramSet) {
        if (paramSet instanceof Map) {
            // TODO: check that all keys are strings
            return (Map<String, Object>) paramSet;
        }

        val mapper = new ObjectMapper(sql);
        return mapper.map(paramSet);
    }

    public Executor withTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }
}
