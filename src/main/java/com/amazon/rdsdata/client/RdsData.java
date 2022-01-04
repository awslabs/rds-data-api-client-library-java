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

import com.amazonaws.services.rdsdata.AWSRDSData;
import com.amazonaws.services.rdsdata.model.BatchExecuteStatementRequest;
import com.amazonaws.services.rdsdata.model.BeginTransactionRequest;
import com.amazonaws.services.rdsdata.model.CommitTransactionRequest;
import com.amazonaws.services.rdsdata.model.DecimalReturnType;
import com.amazonaws.services.rdsdata.model.ExecuteStatementRequest;
import com.amazonaws.services.rdsdata.model.ResultSetOptions;
import com.amazonaws.services.rdsdata.model.RollbackTransactionRequest;
import com.amazonaws.services.rdsdata.model.SqlParameter;
import lombok.Builder;
import lombok.With;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazon.rdsdata.client.MappingOptions.DEFAULT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@Builder
public class RdsData {
    static String ERROR_EMPTY_OR_NULL_SQL = "SQL parameter is null or empty";

    private AWSRDSData sdkClient;
    private String database;
    private String secretArn;
    private String resourceArn;

    @Builder.Default
    @With private MappingOptions mappingOptions = DEFAULT;

    /**
     * Starts a new transaction
     * @return transaction ID
     */
    public String beginTransaction() {
        val request = new BeginTransactionRequest()
                .withDatabase(database)
                .withResourceArn(resourceArn)
                .withSecretArn(secretArn);
        val response = sdkClient.beginTransaction(request);
        return response.getTransactionId();
    }

    /**
     * Commits the given transaction
     * @param transactionId transaction ID
     */
    public void commitTransaction(String transactionId) {
        val request = new CommitTransactionRequest()
                .withTransactionId(transactionId)
                .withResourceArn(resourceArn)
                .withSecretArn(secretArn);
        sdkClient.commitTransaction(request);
    }

    /**
     * Rolls back the given transaction
     * @param transactionId transaction ID
     */
    public void rollbackTransaction(String transactionId) {
        val request = new RollbackTransactionRequest()
                .withTransactionId(transactionId)
                .withResourceArn(resourceArn)
                .withSecretArn(secretArn);
        sdkClient.rollbackTransaction(request);
    }

    /**
     * Creates an {@link Executor} for the given SQL
     * @param sql SQL statement
     * @return an {@link Executor} instance
     * @see Executor
     */
    public Executor forSql(String sql) {
        checkArgument(!isNullOrEmpty(sql), ERROR_EMPTY_OR_NULL_SQL);

        return new Executor(sql, this);
    }

    /**
     * Creates an {@link Executor} for the given SQL with parameters. For each parameter, the SQL statement must
     * contain a placeholder "?"
     * @param sql SQL statement with placeholders
     * @param params vararg array with parameters
     * @return an {@link Executor} instance
     * @see Executor
     */
    public Executor forSql(String sql, Object... params) {
        checkArgument(!isNullOrEmpty(sql), ERROR_EMPTY_OR_NULL_SQL);
        if (params == null) {
            // for case when forSql() is called with one null parameter and Java handled it as the entire vararg is null
            params = new Object[] { null };
        }

        val result = PlaceholderUtils.convertToNamed(sql, params);
        return new Executor(result.sql, this)
                .withParamSets(singletonList(result.parameters));
    }

    ExecutionResult executeStatement(String transactionId, String sql, Map<String, Object> params, boolean continueAfterTimeout) {
        val request = new ExecuteStatementRequest()
                .withDatabase(database)
                .withResourceArn(resourceArn)
                .withSecretArn(secretArn)
                .withSql(sql)
                .withParameters(toSqlParameterList(params))
                .withTransactionId(transactionId)
                .withContinueAfterTimeout(continueAfterTimeout)
                .withResultSetOptions(new ResultSetOptions()
                        .withDecimalReturnType(DecimalReturnType.STRING))
                .withIncludeResultMetadata(true);

        val response = sdkClient.executeStatement(request);

        return new ExecutionResult(response.getColumnMetadata(),
            response.getRecords(),
            response.getNumberOfRecordsUpdated(),
            mappingOptions);
    }

    ExecutionResult batchExecuteStatement(String transactionId, String sql, List<Map<String, Object>> params) {
        val request = new BatchExecuteStatementRequest()
                .withDatabase(database)
                .withResourceArn(resourceArn)
                .withSecretArn(secretArn)
                .withSql(sql)
                .withTransactionId(transactionId)
                .withParameterSets(toSqlParameterSets(params));
        sdkClient.batchExecuteStatement(request);
        return new ExecutionResult(emptyList(), emptyList(), 0L, mappingOptions);
    }

    private List<SqlParameter> toSqlParameterList(Map<String, Object> params) {
        return params.entrySet().stream()
                .map(this::toSqlParameter)
                .collect(toList());
    }

    private SqlParameter toSqlParameter(Map.Entry<String, Object> mapEntry) {
        val parameterName = mapEntry.getKey();
        val value = mapEntry.getValue();

        val parameter = new SqlParameter()
                .withName(parameterName)
                .withValue(TypeConverter.toField(value));

        TypeConverter.getTypeHint(value)
                .ifPresent(hint -> parameter.setTypeHint(hint.name()));

        return parameter;
    }

    private List<List<SqlParameter>> toSqlParameterSets(List<Map<String, Object>> params) {
        return params.stream()
                .map(this::toSqlParameterList)
                .collect(Collectors.toList());
    }
}