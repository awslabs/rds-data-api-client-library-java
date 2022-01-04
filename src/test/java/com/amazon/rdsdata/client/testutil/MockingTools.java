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

import lombok.Value;
import lombok.experimental.UtilityClass;
import lombok.val;
import software.amazon.awssdk.services.rdsdata.RdsDataClient;
import software.amazon.awssdk.services.rdsdata.model.BeginTransactionRequest;
import software.amazon.awssdk.services.rdsdata.model.BeginTransactionResponse;
import software.amazon.awssdk.services.rdsdata.model.ColumnMetadata;
import software.amazon.awssdk.services.rdsdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.rdsdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.rdsdata.model.Field;

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
    public static void mockReturnValue(RdsDataClient mockClient,
                                       long numberOfRecordsUpdated,
                                       ColumnDefinition... columns) {
        mockReturnValues(mockClient, numberOfRecordsUpdated, asList(columns));
    }

    @SafeVarargs
    public static void mockReturnValues(RdsDataClient mockClient,
                                        long numberOfRecordsUpdated,
                                        List<ColumnDefinition>... rows) {
        List<ColumnMetadata> metadataList = rows.length > 0 ? buildColumnMetadataList(rows[0]) : emptyList();

        val recordsList = Stream.of(rows)
                .map(row -> row.stream()
                        .map(c -> c.field)
                        .collect(toList()))
                .collect(toList());

        when(mockClient.executeStatement(any(ExecuteStatementRequest.class)))
                .thenReturn(ExecuteStatementResponse.builder()
                    .columnMetadata(metadataList)
                    .records(recordsList)
                    .numberOfRecordsUpdated(numberOfRecordsUpdated)
                    .build());
    }

    private List<ColumnMetadata> buildColumnMetadataList(List<ColumnDefinition> columns) {
        return columns.stream()
                .map(ColumnDefinition::getMetadata)
                .collect(toList());
    }

    public static ColumnDefinition mockColumn(String name, Field field) {
        val metadata = ColumnMetadata.builder()
            .name(name)
            .build();
        return new ColumnDefinition(metadata, field);
    }

    public static ColumnDefinition mockColumn(String name, String label, Field field) {
        val metadata = ColumnMetadata.builder()
            .name(name)
            .label(label)
            .build();
        return new ColumnDefinition(metadata, field);
    }

    public static void returnNullMetadataAndResultSet(RdsDataClient mockClient) {
        when(mockClient.executeStatement(any(ExecuteStatementRequest.class)))
            .thenReturn(ExecuteStatementResponse.builder()
                .columnMetadata((Collection) null)
                .records((Collection) null)
                .numberOfRecordsUpdated(null)
                .build());
    }

    @Value
    public static class ColumnDefinition {
        private ColumnMetadata metadata;
        private Field field;
    }

    public static void mockBeginTransaction(RdsDataClient mockClient, String transactionId) {
        when(mockClient.beginTransaction(any(BeginTransactionRequest.class)))
            .thenReturn(BeginTransactionResponse.builder()
                .transactionId(transactionId)
                .build());
    }
}
