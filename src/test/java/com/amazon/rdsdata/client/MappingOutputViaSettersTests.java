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
import lombok.NoArgsConstructor;
import lombok.val;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.MappingException.ERROR_NO_FIELD_OR_SETTER;
import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MappingOutputViaSettersTests extends TestBase {
    @Test
    void shouldMapToFieldsOfDifferentType() {
        mockReturnValue(mockColumn("stringValue", new Field().withStringValue("apple")));

        val result = client.forSql("SELECT *")
                .execute()
                .mapToSingle(Setter.class);
        assertThat(result.value).isEqualTo("apple");
    }

    @NoArgsConstructor
    @SuppressWarnings("unused")
    private static class Setter {
        public String value;
        public void setStringValue(String value) { this.value = value; }
    }

    @Test
    void shouldNotUsePrivateSetter() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(PrivateSetter.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_NO_FIELD_OR_SETTER, PrivateSetter.class.getName(), "field");
    }

    @SuppressWarnings("unused")
    public static class PrivateSetter {
        private void setField(String value) { }
    }

    @Test
    void shouldNotUseSetterWithMoreThanOneParameter() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(SetterWithTwoParameters.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_NO_FIELD_OR_SETTER, SetterWithTwoParameters.class.getName(), "field");
    }

    @SuppressWarnings("unused")
    public static class SetterWithTwoParameters {
        public void setField(String value, int secondParameter) { }
    }

    @Test
    void shouldNotUseSetterWithNoParameters() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(SetterWithNoParameters.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_NO_FIELD_OR_SETTER, SetterWithNoParameters.class.getName(), "field");
    }

    @SuppressWarnings("unused")
    public static class SetterWithNoParameters {
        public void setField() { }
    }

    @Test
    void shouldNotUseStaticSetter() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(StaticSetter.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_NO_FIELD_OR_SETTER, StaticSetter.class.getName(), "field");
    }

    @SuppressWarnings("unused")
    public static class StaticSetter {
        public static void setField(String value) { }
    }

    @Test
    void shouldThrowExceptionIfSetterIsNotFound() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(NoCorrespondingSetter.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_NO_FIELD_OR_SETTER, NoCorrespondingSetter.class.getName(), "field");
    }

    @Test
    void shouldNotThrowExceptionIfMissingSetterIsIgnored() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        val mappingOptions = MappingOptions.builder()
            .ignoreMissingSetters(true)
            .build();

        assertThatCode(() -> client.withMappingOptions(mappingOptions).forSql("SELECT *").execute().mapToSingle(NoCorrespondingSetter.class))
            .doesNotThrowAnyException();
    }

    @SuppressWarnings("unused")
    public static class NoCorrespondingSetter {
        public void setAnotherField(String value) { }
    }

    @Test
    void shouldPreferSetterToField() {
        mockReturnValue(mockColumn("field", new Field().withStringValue("apple")));

        val dto = client.forSql("SELECT *").execute().mapToSingle(SetterAndField.class);
        assertThat(dto.field).isEqualTo("orange");
    }

    @SuppressWarnings("unused")
    public static class SetterAndField {
        public String field;
        public void setField(String value) { this.field = "orange"; }
    }

    @Test
    void shouldThrowExceptionIfMoreThanOneSetter() {
        mockReturnValue(mockColumn("field", new Field().withLongValue(123L)));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(AmbiguousSetter.class))
            .isInstanceOf(MappingException.class)
            .hasMessageContaining("Ambiguous setter");
    }

    @SuppressWarnings("unused")
    public static class AmbiguousSetter {
        public void setField(Integer value) { }
        public void setField(Long value) { }
    }
}
