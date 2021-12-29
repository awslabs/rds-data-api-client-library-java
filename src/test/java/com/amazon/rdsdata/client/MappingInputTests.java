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
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.Value;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.amazon.rdsdata.client.FieldMapper.ERROR_FIELD_NOT_FOUND;
import static com.amazon.rdsdata.client.FieldMapper.ERROR_VOID_RETURN_TYPE_NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingInputTests extends TestBase {
    @BeforeEach
    void beforeEach() {
        mockReturnValue(); // return empty response by default
    }

    @Test
    void shouldMapDtoViaGetters() {
        val dto = new Getters();

        client.forSql("INSERT INTO tbl1(a, b) VALUES(:firstName, :lastName)")
                .withParamSets(dto)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("firstName", SdkConstructs.stringField("John")),
            SdkConstructs.parameter("lastName", SdkConstructs.stringField("Doe"))
        );
    }

    @SuppressWarnings("unused")
    private static class Getters {
        public String getFirstName() { return "John"; }
        public String getLastName() { return "Doe"; }
    }

    @Test
    void shouldSupportNullValuesFromGetters() {
        val dto = new NullGetter();

        client.forSql("INSERT INTO tbl1(a) VALUES(:data)")
                .withParamSets(dto)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("data", SdkConstructs.nullField())
        );
    }

    @SuppressWarnings("unused")
    private static class NullGetter {
        public String getData() { return null; }
    }

    @Test
    void shouldMapDtoViaFields() {
        val dto = new Fields();

        client.forSql("INSERT INTO tbl1(a, b) VALUES(:firstName, :lastName)")
                .withParamSets(dto)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("firstName", SdkConstructs.stringField("John")),
            SdkConstructs.parameter("lastName", SdkConstructs.stringField("Doe"))
        );
    }

    @SuppressWarnings("unused")
    private static class Fields {
        private final String firstName = "John";
        private final String lastName = "Doe";
    }

    @Test
    void shouldSupportNullValuesFromFields() {
        val dto = new NullField();

        client.forSql("INSERT INTO tbl1(a) VALUES(:data)")
                .withParamSets(dto)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("data", SdkConstructs.nullField())
        );
    }

    @SuppressWarnings("unused")
    private static class NullField {
        private final String data = null;
    }

    @Test
    void shouldMapDtoWithBothBoxedAndUnboxedFields() {
        val dto = new BoxedAndUnboxedInts();

        client.forSql("INSERT INTO tbl1(a, b) VALUES(:boxed, :unboxed)")
                .withParamSets(dto)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("unboxed", SdkConstructs.longField(1L)),
            SdkConstructs.parameter("boxed", SdkConstructs.longField(2L))
        );
    }

    @SuppressWarnings("unused")
    private static class BoxedAndUnboxedInts {
        private final int unboxed = 1;
        private final Integer boxed = 2;
    }

    @Test
    void shouldThrowExceptionIfFieldNotFound() {
        assertThatThrownBy(() ->
            client.forSql("INSERT INTO tbl1(a) VALUES(:fn)")
                    .withParamSets(new Object())
                    .execute())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(ERROR_FIELD_NOT_FOUND.substring(0, 40));
    }

    @Test
    void shouldThrowExceptionIfFieldReturnTypeIsVoid() {
        val dto = new VoidGetter();
        assertThatThrownBy(() ->
                client.forSql("INSERT INTO tbl1(a) VALUES(:void)")
                        .withParamSets(dto)
                        .execute())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ERROR_VOID_RETURN_TYPE_NOT_SUPPORTED);
    }

    @SuppressWarnings("unused")
    private static class VoidGetter {
        public void getVoid() { }
    }

    @Test
    void shouldThrowNotFoundExceptionIfFieldNameIsANumber() {
        assertThatThrownBy(() ->
                client.forSql("INSERT INTO tbl1(a) VALUES(:333)")
                        .withParamSets(new Object())
                        .execute())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(ERROR_FIELD_NOT_FOUND.substring(0, 40));
    }

    @Test
    @Disabled // TODO: handle getters of type "is...()"
    void shouldMapDtoIsBooleanGetter() {
        client.forSql("INSERT INTO tbl1(a) VALUES(:enabled)")
                .withParamSets(new IsGetter())
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("enabled", SdkConstructs.booleanField(true))
        );
    }

    @SuppressWarnings("unused")
    private static class IsGetter {
        public boolean isEnabled() { return true;}
    }

    @Test
    void shouldMapDtosForBatchUpdate() {
        val dto1 = new DtoForBatch(1);
        val dto2 = new DtoForBatch(2);

        client.forSql("INSERT INTO tbl1(a) VALUES(:value)")
                .withParamSets(dto1, dto2)
                .execute();

        val request = captureBatchRequest();
        assertThat(request.getParameterSets()).containsExactly(
                // expecting list of lists of parameters
                ImmutableList.of(
                    SdkConstructs.parameter("value", SdkConstructs.longField(1L))
                ),
                ImmutableList.of(
                    SdkConstructs.parameter("value", SdkConstructs.longField(2L))
                )
        );
    }

    @Value
    private static class DtoForBatch {
        public final int value;
    }

    @Test
    void shouldMapInputParameterWhenOneParamSetIsPassed() {
        val dto = new SampleDto("value1");

        client.forSql("SELECT * FROM table WHERE param1 = :param1")
                .withParameter(dto)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("param1", SdkConstructs.stringField("value1"))
        );
    }

    @Value
    private static class SampleDto {
        public final String param1;
    }

    @Test
    void shouldMapDtoWithFieldInChildClass() {
        val dto = new ParentWithNoFields();

        client.forSql("INSERT INTO tbl1(value) VALUES(:value)")
            .withParamSets(dto)
            .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("value", SdkConstructs.longField(1L))
        );
    }

    private static class ParentWithNoFields extends ChildWithField {
    }

    private static class ChildWithField {
        @SuppressWarnings("unused")
        public final int value = 1;
    }

    @Test
    void shouldMapDtoWithMethodInChildClass() {
        val dto = new ParentWithNoMethods();

        client.forSql("INSERT INTO tbl1(value) VALUES(:value)")
            .withParamSets(dto)
            .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactlyInAnyOrder(
            SdkConstructs.parameter("value", SdkConstructs.longField(1L))
        );
    }

    private static class ParentWithNoMethods extends ChildWithMethod {
    }

    private static class ChildWithMethod {
        @SuppressWarnings("unused")
        @Getter private final int value = 1;
    }
}