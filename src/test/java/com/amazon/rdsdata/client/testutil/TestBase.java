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

import com.amazon.rdsdata.client.RdsData;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.rdsdata.RdsDataClient;
import software.amazon.awssdk.services.rdsdata.model.BatchExecuteStatementRequest;
import software.amazon.awssdk.services.rdsdata.model.BeginTransactionRequest;
import software.amazon.awssdk.services.rdsdata.model.CommitTransactionRequest;
import software.amazon.awssdk.services.rdsdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.rdsdata.model.RollbackTransactionRequest;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestBase {
    protected static final String SAMPLE_DB = "mydb";
    protected static final String SAMPLE_RESOURCE_ARN = "arn:resource";
    protected static final String SAMPLE_SECRET_ARN = "arn:secret";

    protected RdsData client;
    protected RdsDataClient sdkClient = mock(RdsDataClient.class);

    @BeforeEach
    void createClient() {
        client = RdsData.builder()
                .sdkClient(sdkClient)
                .database(SAMPLE_DB)
                .resourceArn(SAMPLE_RESOURCE_ARN)
                .secretArn(SAMPLE_SECRET_ARN)
                .build();
    }

    protected void mockReturnValue(MockingTools.ColumnDefinition... columns) {
        MockingTools.mockReturnValue(sdkClient, 0L, columns);
    }


    protected void mockReturnValue(long numberOfRecordsUpdated, MockingTools.ColumnDefinition... columns) {
        MockingTools.mockReturnValue(sdkClient, numberOfRecordsUpdated, columns);
    }

    @SafeVarargs
    protected final void mockReturnValues(List<MockingTools.ColumnDefinition>... rows) {
        MockingTools.mockReturnValues(sdkClient, 0L, rows);
    }

    protected final void returnNullMetadataAndResultSet() {
        MockingTools.returnNullMetadataAndResultSet(sdkClient);
    }

    protected ExecuteStatementRequest captureRequest() {
        val captor = ArgumentCaptor.forClass(ExecuteStatementRequest.class);
        verify(sdkClient).executeStatement(captor.capture());
        return captor.getValue();
    }

    protected BatchExecuteStatementRequest captureBatchRequest() {
        val captor = ArgumentCaptor.forClass(BatchExecuteStatementRequest.class);
        verify(sdkClient).batchExecuteStatement(captor.capture());
        return captor.getValue();
    }

    protected BeginTransactionRequest captureBeginTransactionRequest() {
        val captor = ArgumentCaptor.forClass(BeginTransactionRequest.class);
        verify(sdkClient).beginTransaction(captor.capture());
        return captor.getValue();
    }

    protected CommitTransactionRequest captureCommitTransactionRequest() {
        val captor = ArgumentCaptor.forClass(CommitTransactionRequest.class);
        verify(sdkClient).commitTransaction(captor.capture());
        return captor.getValue();
    }

    protected RollbackTransactionRequest captureRollbackTransactionRequest() {
        val captor = ArgumentCaptor.forClass(RollbackTransactionRequest.class);
        verify(sdkClient).rollbackTransaction(captor.capture());
        return captor.getValue();
    }
}