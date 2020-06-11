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
import lombok.Value;
import lombok.val;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MapToSingleTests extends TestBase {
    @Test
    void shouldMapToSingleObject() {
        mockReturnValue(
                mockColumn("intField", new Field().withLongValue(1L)),
                mockColumn("stringField", new Field().withStringValue("hello")));

        val result = client.forSql("SELECT *")
                .execute()
                .mapToSingle(TestBean.class);
        assertThat(result).isEqualTo(new TestBean(1, "hello"));
    }

    @Value
    private static class TestBean {
        public final int intField;
        public final String stringField;
    }

    @Test
    void shouldThrowExceptionIfNoResults() {
        mockReturnValues(); // client returns empty result set

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(Object.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(MappingException.ERROR_EMPTY_RESULT_SET);
    }

    @Test
    void shouldTolerateNullMetadata() {
        returnNullMetadataAndResultSet();

        // DML value results usually do not contain metadata and values
        assertThatCode(() -> client.forSql("INSERT INTO tbl VALUES(1)").execute())
                .doesNotThrowAnyException();
    }

}
