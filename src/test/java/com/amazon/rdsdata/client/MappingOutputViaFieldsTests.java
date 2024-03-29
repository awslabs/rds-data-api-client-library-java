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
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.val;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.MappingException.ERROR_CANNOT_ACCESS_FIELD;
import static com.amazon.rdsdata.client.MappingException.ERROR_CANNOT_CREATE_INSTANCE;
import static com.amazon.rdsdata.client.MappingException.ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS;
import static com.amazon.rdsdata.client.MappingException.ERROR_NO_FIELD_OR_SETTER;
import static com.amazon.rdsdata.client.MappingException.ERROR_STATIC_FIELD;
import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MappingOutputViaFieldsTests extends TestBase {
    @Test
    void shouldMapToClassWithPublicFields() {
        mockReturnValue(mockColumn("value", SdkConstructs.stringField("apple")));

        val result = client.forSql("SELECT *")
                .execute()
                .mapToSingle(PublicFields.class);
        assertThat(result.value).isEqualTo("apple");
    }

    @NoArgsConstructor
    private static class PublicFields {
        public String value;
    }

    @Test
    void shouldThrowExceptionIfFieldDoesNotExist() {
        mockReturnValue(mockColumn("field", SdkConstructs.stringField("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(Object.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_NO_FIELD_OR_SETTER, Object.class.getName(), "field");
    }

    @Test
    void shouldThrowExceptionIfFieldIsPrivate() {
        mockReturnValue(mockColumn("field", SdkConstructs.stringField("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(PrivateField.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_ACCESS_FIELD, "field", PrivateField.class.getName());
    }

    @SuppressWarnings("unused")
    public static class PrivateField {
        private String field;
    }

    @Test
    void shouldThrowExceptionIfFieldIsFinal() {
        mockReturnValue(mockColumn("field", SdkConstructs.stringField("apple")));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(FinalField.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_ACCESS_FIELD, "field", FinalField.class.getName());
    }

    public static class FinalField {
        public final String field = "grape";
    }

    @Test
    void shouldThrowExceptionIfConstructorIsNotAccessible() {
        mockReturnValue();

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(PrivateConstructor.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CREATE_INSTANCE, PrivateConstructor.class.getName());
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class PrivateConstructor {
    }

    @Test
    void shouldThrowExceptionIfFieldIsStatic() {
        mockReturnValue(mockColumn("field", SdkConstructs.longField(1111L)));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(StaticField.class))
                .isInstanceOf(MappingException.class)
                .hasMessageStartingWith(format(ERROR_STATIC_FIELD, "field", StaticField.class.getName()));
    }

    @SuppressWarnings("unused")
    public static class StaticField {
        public static int field;
    }

    @Test
    void shouldUseFieldIfSetterIsNotAvailable() {
        mockReturnValue(mockColumn("field", SdkConstructs.longField(333L)));

        val dto = client.forSql("SELECT *").execute().mapToSingle(SetterAndField.class);
        assertThat(dto.field).isEqualTo(333L);
    }

    public static class SetterAndField {
        public int field;
        private void setField(int value) { this.field = -1; }
    }

    @Test
    void shouldThrowExceptionIfNoArgsConstructorNotFound() {
        mockReturnValue(mockColumn("field", SdkConstructs.longField(333L)));

        assertThatThrownBy(() -> client.forSql("SELECT *").execute().mapToSingle(WithoutNoArgConstructor.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CREATE_INSTANCE_VIA_NOARGS, WithoutNoArgConstructor.class.getName());
    }

    @SuppressWarnings("unused")
    public static class WithoutNoArgConstructor {
        public int field;
        public WithoutNoArgConstructor(int x) { }
    }

    @Test
    void shouldAccessFieldsFromParentClass() {
        mockReturnValue(mockColumn("field", SdkConstructs.stringField("apple")));

        assertThatCode(() -> client.forSql("SELECT *").execute().mapToSingle(ChildWithoutFields.class))
            .doesNotThrowAnyException();
    }

    public static class ChildWithoutFields extends ParentWithField {
    }

    public static class ParentWithField {
        public String field;
    }
}
