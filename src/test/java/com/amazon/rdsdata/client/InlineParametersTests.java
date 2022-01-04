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

import static com.amazon.rdsdata.client.PlaceholderUtils.ERROR_NUMBER_OF_PARAMS_MISMATCH;
import static com.amazon.rdsdata.client.RdsData.ERROR_EMPTY_OR_NULL_SQL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class InlineParametersTests extends TestBase {
    @BeforeEach
    public void beforeEach() {
        mockReturnValue(); // return empty response by default
    }

    @Test
    void shouldReplaceQuestionMarksWithNamedPlaceholders() {
        client.forSql("INSERT INTO tbl1(a, b, c) VALUES(?, ?, ?)", 1, 2, 3)
                .execute();

        val request = captureRequest();
        assertThat(request.sql()).isEqualTo("INSERT INTO tbl1(a, b, c) VALUES(:1, :2, :3)");
    }

    @Test
    void shouldSupportNoParameters() {
        client.forSql("SELECT 1").execute();

        val request = captureRequest();
        assertThat(request.sql()).isEqualTo("SELECT 1");
        assertThat(request.parameters()).isEmpty();
    }

    @Test
    void shouldThrowExceptionIfSqlIsNull() {
        assertThatThrownBy(() -> client.forSql(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ERROR_EMPTY_OR_NULL_SQL);
    }

    @Test
    void shouldThrowExceptionIfSqlIsNullWithParams() {
        assertThatThrownBy(() -> client.forSql(null, 1, 2, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ERROR_EMPTY_OR_NULL_SQL);
    }

    @Test
    void shouldThrowExceptionIfNumberOfParametersDontMatchNumberOfPlaceholders() {
        assertThatThrownBy(() -> client.forSql("INSERT INTO tbl1(a, b, c) VALUES(?, ?, ?)", 1, 2, 3, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ERROR_NUMBER_OF_PARAMS_MISMATCH);
    }
}
