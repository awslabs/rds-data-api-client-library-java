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
import lombok.val;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.MappingException.ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS;
import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MappingOutputViaAllArgsConstructorTests extends TestBase {
    @Test
    void shouldMapViaAllArgsConstructor() {
        mockReturnValue(NUMBER_OF_RECORDS_UPDATED,
                mockColumn("stringValue", new Field().withStringValue("apple")),
                mockColumn("intValue", new Field().withLongValue(15L)));

        val executionResult = client.forSql("SELECT *").execute();
        assertEquals(NUMBER_OF_RECORDS_UPDATED, executionResult.getNumberOfRecordsUpdated());

        val result = executionResult.mapToSingle(ConstructorWithParameterNames.class);
        assertThat(result.result).isEqualTo("apple15");
    }

    private static class ConstructorWithParameterNames {
        public final String result;
        public ConstructorWithParameterNames(String stringValue, int intValue) {
            this.result = stringValue + intValue;
        }
    }

    @Test
    void shouldFailToMapIfConstructorHasMoreParameters() {
        mockReturnValue(mockColumn("stringValue", new Field().withStringValue("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(ConstructorWithExtraParameters.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS, ConstructorWithExtraParameters.class.getName());
    }

    private static class ConstructorWithExtraParameters {
        @SuppressWarnings("unused")
        public ConstructorWithExtraParameters(String stringValue, int intValue) { }
    }

    @Test
    void shouldFailToMapIfConstructorHasLessParameters() {
        mockReturnValue(
                mockColumn("stringValue1", new Field().withStringValue("apple")),
                mockColumn("stringValue2", new Field().withStringValue("orange")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(ConstructorWithLessParameters.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS, ConstructorWithLessParameters.class.getName());
    }

    private static class ConstructorWithLessParameters {
        @SuppressWarnings("unused")
        public ConstructorWithLessParameters(String stringValue1) { }
    }
}
