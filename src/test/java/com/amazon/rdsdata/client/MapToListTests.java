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
import com.amazonaws.services.rdsdata.model.Field;
import com.google.common.collect.ImmutableList;
import lombok.Value;
import lombok.val;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;

public class MapToListTests extends TestBase {
    @Test
    void shouldMapViaAllArgsConstructor() {
        mockReturnValues(NUMBER_OF_RECORDS_UPDATED,
                ImmutableList.of( // first row
                        mockColumn("intField", new Field().withLongValue(1L)),
                        mockColumn("stringField", new Field().withStringValue("hello"))
                ), ImmutableList.of( // 2nd row
                        mockColumn("intField", new Field().withLongValue(2L)),
                        mockColumn("stringField", new Field().withStringValue("world"))
                ));

        val executionResult = client.forSql("SELECT *").execute();
        assertEquals(NUMBER_OF_RECORDS_UPDATED, executionResult.getNumberOfRecordsUpdated());

        val result = executionResult.mapToList(TestBean.class);
        assertThat(result).containsExactly(
                new TestBean(1, "hello"),
                new TestBean(2, "world"));
    }

    @Value
    private static class TestBean {
        public final int intField;
        public final String stringField;
    }
}
