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

import com.amazon.rdsdata.client.testutil.TestBase;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static com.amazon.rdsdata.client.testutil.MockingTools.mockBeginTransaction;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class TransactionTests extends TestBase {
    @BeforeEach
    void beforeEach() {
        mockReturnValue(); // return empty response by default
    }

    @Test
    public void shouldPropagateTransactionId() {
        val transactionId = UUID.randomUUID().toString();

        client.forSql("SELECT *")
                .withTransactionId(transactionId)
                .execute();

        val request = captureRequest();
        assertThat(request.transactionId()).isEqualTo(transactionId);
    }

    @Test
    public void shouldPropagateTransactionIdForBatch() {
        val transactionId = UUID.randomUUID().toString();

        client.forSql("SELECT *")
                .withTransactionId(transactionId)
                .withParamSets(emptyList(), emptyList())
                .execute();

        val request = captureBatchRequest();
        assertThat(request.transactionId()).isEqualTo(transactionId);
    }

    @Test
    public void shouldBeginTransaction() {
        val transactionId = UUID.randomUUID().toString();
        mockBeginTransaction(sdkClient, transactionId);

        val result = client.beginTransaction();
        assertThat(result).isEqualTo(transactionId);

        val request = captureBeginTransactionRequest();
        assertThat(request.database()).isEqualTo(SAMPLE_DB);
        assertThat(request.resourceArn()).isEqualTo(SAMPLE_RESOURCE_ARN);
        assertThat(request.secretArn()).isEqualTo(SAMPLE_SECRET_ARN);
    }

    @Test
    public void shouldCommitTransaction() {
        val transactionId = UUID.randomUUID().toString();

        client.commitTransaction(transactionId);

        val request = captureCommitTransactionRequest();
        assertThat(request.transactionId()).isEqualTo(transactionId);
        assertThat(request.resourceArn()).isEqualTo(SAMPLE_RESOURCE_ARN);
        assertThat(request.secretArn()).isEqualTo(SAMPLE_SECRET_ARN);
    }

    @Test
    public void shouldRollbackTransaction() {
        val transactionId = UUID.randomUUID().toString();

        client.rollbackTransaction(transactionId);

        val request = captureRollbackTransactionRequest();
        assertThat(request.transactionId()).isEqualTo(transactionId);
        assertThat(request.resourceArn()).isEqualTo(SAMPLE_RESOURCE_ARN);
        assertThat(request.secretArn()).isEqualTo(SAMPLE_SECRET_ARN);
    }
}
