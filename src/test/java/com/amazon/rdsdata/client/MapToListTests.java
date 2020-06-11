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
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;

public class MapToListTests extends TestBase {
    @Test
    void shouldMapViaAllArgsConstructor() {
        mockReturnValues(
                ImmutableList.of( // first row
                        mockColumn("intField", new Field().withLongValue(1L)),
                        mockColumn("stringField", new Field().withStringValue("hello"))
                ), ImmutableList.of( // 2nd row
                        mockColumn("intField", new Field().withLongValue(2L)),
                        mockColumn("stringField", new Field().withStringValue("world"))
                ));

        val result = client.forSql("SELECT *")
                .execute()
                .mapToList(TestBean.class);

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
