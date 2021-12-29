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

import com.amazon.rdsdata.client.testutil.SdkConstructs;
import com.amazon.rdsdata.client.testutil.TestBase;
import lombok.val;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.MappingException.ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS;
import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MappingOutputViaAllArgsConstructorTests extends TestBase {
    @Test
    void shouldMapViaAllArgsConstructor() {
        mockReturnValue(
                mockColumn("stringValue", SdkConstructs.stringField("apple")),
                mockColumn("intValue", SdkConstructs.longField(15L)));

        val result = client.forSql("SELECT *")
                .execute()
                .mapToSingle(ConstructorWithParameterNames.class);
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
        mockReturnValue(mockColumn("stringValue", SdkConstructs.stringField("apple")));

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
                mockColumn("stringValue1", SdkConstructs.stringField("apple")),
                mockColumn("stringValue2", SdkConstructs.stringField("orange")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(ConstructorWithLessParameters.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS, ConstructorWithLessParameters.class.getName());
    }

    private static class ConstructorWithLessParameters {
        @SuppressWarnings("unused")
        public ConstructorWithLessParameters(String stringValue1) { }
    }
}
