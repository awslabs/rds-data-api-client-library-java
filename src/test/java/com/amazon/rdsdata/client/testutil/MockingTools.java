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
package com.amazon.rdsdata.client.testutil;

import com.amazonaws.services.rdsdata.AWSRDSData;
import com.amazonaws.services.rdsdata.model.BeginTransactionRequest;
import com.amazonaws.services.rdsdata.model.BeginTransactionResult;
import com.amazonaws.services.rdsdata.model.ColumnMetadata;
import com.amazonaws.services.rdsdata.model.ExecuteStatementRequest;
import com.amazonaws.services.rdsdata.model.ExecuteStatementResult;
import com.amazonaws.services.rdsdata.model.Field;
import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.val;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@UtilityClass
public class MockingTools {
    public static void mockReturnValue(AWSRDSData mockClient,
                                       long numberOfRecordsUpdated,
                                       ColumnDefinition... columns) {
        mockReturnValues(mockClient, numberOfRecordsUpdated, asList(columns));
    }

    @SafeVarargs
    public static void mockReturnValues(AWSRDSData mockClient,
                                        long numberOfRecordsUpdated,
                                        List<ColumnDefinition>... rows) {
        List<ColumnMetadata> metadataList = rows.length > 0 ? buildColumnMetadataList(rows[0]) : emptyList();

        val recordsList = Stream.of(rows)
                .map(row -> row.stream()
                        .map(c -> c.field)
                        .collect(toList()))
                .collect(toList());

        when(mockClient.executeStatement(any(ExecuteStatementRequest.class)))
                .thenReturn(new ExecuteStatementResult()
                        .withColumnMetadata(metadataList)
                        .withRecords(recordsList)
                        .withNumberOfRecordsUpdated(numberOfRecordsUpdated));
    }

    private List<ColumnMetadata> buildColumnMetadataList(List<ColumnDefinition> columns) {
        return columns.stream()
                .map(ColumnDefinition::getMetadata)
                .collect(toList());
    }

    public static ColumnDefinition mockColumn(String name, Field field) {
        val metadata = new ColumnMetadata()
            .withName(name);
        return new ColumnDefinition(metadata, field);
    }

    public static ColumnDefinition mockColumn(String name, String label, Field field) {
        val metadata = new ColumnMetadata()
            .withName(name)
            .withLabel(label);
        return new ColumnDefinition(metadata, field);
    }

    public static void returnNullMetadataAndResultSet(AWSRDSData mockClient) {
        when(mockClient.executeStatement(any(ExecuteStatementRequest.class)))
                .thenReturn(new ExecuteStatementResult()
                        .withColumnMetadata((Collection) null)
                        .withRecords((Collection) null)
                        .withNumberOfRecordsUpdated(null));
    }

    @Value
    public static class ColumnDefinition {
        private ColumnMetadata metadata;
        private Field field;
    }

    public static void mockBeginTransaction(AWSRDSData mockClient, String transactionId) {
        when(mockClient.beginTransaction(any(BeginTransactionRequest.class)))
            .thenReturn(new BeginTransactionResult().withTransactionId(transactionId));
    }
}
