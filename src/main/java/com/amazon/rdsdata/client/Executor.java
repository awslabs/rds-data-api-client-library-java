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

import java.util.HashMap;
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
    private final RdsData rdsData;
    private List<Object> paramSets = emptyList();
    private String transactionId = ""; // RDS Data API understands empty string as "no transaction"
    private boolean continueAfterTimeout = false;

    /**
     * Sets a single parameter set
     * @param param object which fields will be used as a source for parameters
     * @return a reference to this object so that method calls can be chained together
     */
    public Executor withParameter(Object param) {
        this.paramSets = singletonList(param);
        return this;
    }

    /**
     * Sets multiple parameter sets
     * @param params {@link List} of objects which fields will be used as sources for parameters
     * @return a reference to this object so that method calls can be chained together
     */
    public Executor withParamSets(List<Object> params) {
        this.paramSets = params;
        return this;
    }

    /**
     * Sets multiple parameter sets
     * @param params vararg array of objects which fields will be sources for parameters
     * @return a reference to this object so that method calls can be chained together
     */
    public Executor withParamSets(Object... params) {
        return withParamSets(asList(params));
    }

    /**
     * Sets a single named parameter.
     * Should not be combined with {@link #withParameter(Object)} and {@link #withParamSets(Object...)}
     * @param parameterName Name of the parameter
     * @param value value (can be of any supported type)
     * @return a reference to this object so that method calls can be chained together
     */
    public Executor withParameter(String parameterName, Object value) {
        if (paramSets.isEmpty()) {
            paramSets = singletonList(new HashMap<String, Object>());
        }

        val firstParamSet = paramSets.get(0);
        if (!(firstParamSet instanceof Map)) {
            throw new IllegalArgumentException("Parameters are already supplied");
        }

        //noinspection unchecked
        ((Map<String, Object>) paramSets.get(0)).put(parameterName, value);
        return this;
    }

    /**
     * Executes the SQL query.
     *
     * If only one parameter set was added to this {@link Executor} before, or no parameters at all,
     * ExecuteStatement API will be called
     *
     * If more than one parameter set was added (via <code>withParamSets()</code> methods),
     * BatchExecuteStatement API will be used
     *
     * @return a {@link ExecutionResult} instance
     */
    public ExecutionResult execute() {
        return paramSets.size() > 1 ? executeAsBatch() : executeAsSingle();
    }

    private ExecutionResult executeAsBatch() {
        val paramSetsAsMaps = paramSets.stream()
                .map(paramSet -> toMap(sql, paramSet))
                .collect(toList());
        return rdsData.batchExecuteStatement(transactionId, sql, paramSetsAsMaps);
    }

    private ExecutionResult executeAsSingle() {
        val firstParamSetAsMap = paramSets.stream()
                .findFirst()
                .map(paramSet -> toMap(sql, paramSet))
                .orElse(emptyMap());
        return rdsData.executeStatement(transactionId, sql, firstParamSetAsMap, continueAfterTimeout);
    }

    private Map<String, Object> toMap(String sql, Object paramSet) {
        if (paramSet instanceof Map) {
            // TODO: check that all keys are strings
            return (Map<String, Object>) paramSet;
        }

        val mapper = new ObjectMapper(sql);
        return mapper.map(paramSet);
    }

    /**
     * Specifies that the query should be executed in a transaction
     * @param transactionId transaction ID
     * @return a reference to this object so that method calls can be chained together
     */
    public Executor withTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    /**
     * Specifies that the query should continue to be executed even after timeout
     * @return a reference to this object so that method calls can be chained
     */
    public Executor withContinueAfterTimeout() {
        this.continueAfterTimeout = true;
        return this;
    }
}
