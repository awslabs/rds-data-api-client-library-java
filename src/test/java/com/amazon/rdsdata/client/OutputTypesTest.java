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
import lombok.NoArgsConstructor;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import static com.amazon.rdsdata.client.MappingException.ERROR_CANNOT_CONVERT_TO_TYPE;
import static com.amazon.rdsdata.client.testutil.MockingTools.mockColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OutputTypesTest extends TestBase {
    @Test
    void shouldMapToFieldsOfDifferentType() {
        val bytes = new byte[] {1, 2, 3};
        val uuid = UUID.randomUUID();
        mockReturnValue(
                mockColumn("stringValue", SdkConstructs.stringField("apple")),
                mockColumn("byteValue", SdkConstructs.longField(3L)),
                mockColumn("intValue", SdkConstructs.longField(4L)),
                mockColumn("longValue", SdkConstructs.longField(5L)),
                mockColumn("charValue", SdkConstructs.longField(6L)),
                mockColumn("boxedLongValue", SdkConstructs.longField(7L)),
                mockColumn("doubleValue", SdkConstructs.doubleField(1.5d)),
                mockColumn("floatValue", SdkConstructs.doubleField(2.5d)),
                mockColumn("blob", SdkConstructs.blobField(bytes)),
                mockColumn("booleanValue", SdkConstructs.booleanField(true)),
                mockColumn("nullField", SdkConstructs.nullField()),
                mockColumn("enumType", SdkConstructs.stringField("VALUE_1")),
                mockColumn("uuid", SdkConstructs.stringField(uuid.toString()))
        );

        val result = client.forSql("SELECT *")
                .execute()
                .mapToSingle(FieldsOfDifferentTypes.class);

        assertThat(result.stringValue).isEqualTo("apple");
        assertThat(result.byteValue).isEqualTo((byte) 3);
        assertThat(result.intValue).isEqualTo(4);
        assertThat(result.longValue).isEqualTo(5L);
        assertThat(result.charValue).isEqualTo((char) 6);
        assertThat(result.boxedLongValue).isEqualTo(7L);
        assertThat(result.doubleValue).isEqualTo(1.5d);
        assertThat(result.floatValue).isEqualTo(2.5d);
        assertThat(result.blob).isEqualTo(bytes);
        assertThat(result.booleanValue).isEqualTo(true);
        assertThat(result.nullField).isNull();
        assertThat(result.enumType).isEqualTo(EnumType.VALUE_1);
        assertThat(result.uuid).isEqualTo(uuid);
    }

    @NoArgsConstructor
    private static class FieldsOfDifferentTypes {
        public String stringValue;
        public byte byteValue;
        public int intValue;
        public long longValue;
        public char charValue;
        public Long boxedLongValue;
        public double doubleValue;
        public double floatValue;
        public byte[] blob;
        public boolean booleanValue;
        public String nullField;
        public EnumType enumType;
        public UUID uuid;
    }

    @Test
    void shouldMapToFieldsOfTemporalTypes() {
        mockReturnValue(
            mockColumn("localDateTime", SdkConstructs.stringField("2021-08-23 14:30:16.223")),
            mockColumn("localDateFromDateWithTime", SdkConstructs.stringField("2021-08-23 14:30:16.223")),
            mockColumn("localDateFromDate", SdkConstructs.stringField("2021-08-23")),
            mockColumn("localTimeFromDateWithTime", SdkConstructs.stringField("2021-08-23 14:30:16.223")),
            mockColumn("localTimeFromTime", SdkConstructs.stringField("14:30:16"))
        );

        val result = client.forSql("SELECT *")
            .execute()
            .mapToSingle(TemporalTypes.class);

        assertThat(result.localDateTime).isEqualTo(LocalDateTime.of(2021, 8, 23, 14, 30, 16, 223_000_000));
        assertThat(result.localDateFromDateWithTime).isEqualTo(LocalDate.of(2021, 8, 23));
        assertThat(result.localDateFromDate).isEqualTo(LocalDate.of(2021, 8, 23));
        assertThat(result.localTimeFromDateWithTime).isEqualTo(LocalTime.of(14, 30, 16, 223_000_000));
        assertThat(result.localTimeFromTime).isEqualTo(LocalTime.of(14, 30, 16));
    }

    @NoArgsConstructor
    private static class TemporalTypes {
        public LocalDateTime localDateTime;
        public LocalDate localDateFromDateWithTime;
        public LocalDate localDateFromDate;
        public LocalTime localTimeFromDateWithTime;
        public LocalTime localTimeFromTime;
    }

    @Test
    void shouldMapToFieldsOfDecimalTypes() {
        mockReturnValue(
                mockColumn("bigDecimalFromString", SdkConstructs.stringField("12.25")),
                mockColumn("bigDecimalFromLong", SdkConstructs.longField(12L)),
                mockColumn("bigDecimalFromDouble", SdkConstructs.doubleField(12.5)),
                mockColumn("bigIntegerFromString", SdkConstructs.stringField("333")),
                mockColumn("bigIntegerFromLong", SdkConstructs.longField(444L))
        );

        val result = client.forSql("SELECT *")
                .execute()
                .mapToSingle(DecimalTypes.class);

        assertThat(result.bigDecimalFromString).isEqualTo(BigDecimal.valueOf(1225, 2));
        assertThat(result.bigDecimalFromLong).isEqualTo(BigDecimal.valueOf(12));
        assertThat(result.bigDecimalFromDouble).isEqualTo(BigDecimal.valueOf(12.5));
        assertThat(result.bigIntegerFromString).isEqualTo(BigInteger.valueOf(333));
        assertThat(result.bigIntegerFromLong).isEqualTo(BigInteger.valueOf(444));
    }

    @NoArgsConstructor
    private static class DecimalTypes {
        public BigDecimal bigDecimalFromString;
        public BigDecimal bigDecimalFromLong;
        public BigDecimal bigDecimalFromDouble;
        public BigInteger bigIntegerFromString;
        public BigInteger bigIntegerFromLong;
    }

    @Test
    void shouldThrowExceptionIfFieldCannotBeConvertedToBigDecimal() {
        val field = SdkConstructs.booleanField(true);
        mockReturnValue(mockColumn("bigDecimal", field));

        assertThatThrownBy(() -> client.forSql("SELECT *")
                .execute()
                .mapToSingle(DtoWithDecimalAndInteger.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CONVERT_TO_TYPE, field, BigDecimal.class.toString());
    }

    @NoArgsConstructor
    @SuppressWarnings("unused")
    private static class DtoWithDecimalAndInteger {
        public BigDecimal bigDecimal;
        public BigInteger bigInteger;
    }

    @Test
    void shouldThrowExceptionIfFieldCannotBeConvertedToBigInteger() {
        val field = SdkConstructs.booleanField(true);
        mockReturnValue(mockColumn("bigInteger", field));

        assertThatThrownBy(() -> client.forSql("SELECT *")
                .execute()
                .mapToSingle(DtoWithDecimalAndInteger.class))
                .isInstanceOf(MappingException.class)
                .hasMessage(ERROR_CANNOT_CONVERT_TO_TYPE, field, BigInteger.class.toString());
    }

    private enum EnumType {
        VALUE_1
    }
}
