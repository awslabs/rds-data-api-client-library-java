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
import com.amazonaws.services.rdsdata.model.SqlParameter;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static com.amazon.rdsdata.client.TypeConverter.DATE_FORMATTER;
import static com.amazon.rdsdata.client.TypeConverter.DATE_TIME_FORMATTER;
import static com.amazon.rdsdata.client.TypeConverter.ERROR_PARAMETER_OF_UNKNOWN_TYPE;
import static com.amazon.rdsdata.client.TypeConverter.TIME_FORMATTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class InputTypesTest extends TestBase {
    @BeforeEach
    public void beforeEach() {
        mockReturnValue(); // return empty response by default
    }

    @ParameterizedTest
    @MethodSource("typesThatDontNeedHints")
    void shouldSupportDifferentTypesOfParameters(Object parameter, Field expectedValue) {
        client.forSql("INSERT INTO tbl1(a) VALUES(?)", parameter)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactly(
                new SqlParameter()
                        .withName("1")
                        .withValue(expectedValue)
        );
    }

    @SuppressWarnings("RedundantCast")
    private static Stream<Arguments> typesThatDontNeedHints() {
        val bytes = new byte[] {1, 2, 3};
        return Stream.of(
                arguments((byte) 11, new Field().withLongValue(11L)),
                arguments((int) 12, new Field().withLongValue(12L)),
                arguments((long) 13, new Field().withLongValue(13L)),
                arguments((char) 14, new Field().withLongValue(14L)),
                arguments(1.5f, new Field().withDoubleValue(1.5d)),
                arguments(2.5d, new Field().withDoubleValue(2.5d)),
                arguments("hello", new Field().withStringValue("hello")),
                arguments(bytes, new Field().withBlobValue(ByteBuffer.wrap(bytes))),
                arguments(true, new Field().withBooleanValue(true))
        );
    }

    @ParameterizedTest
    @MethodSource("typesThatNeedHints")
    void shouldSupportDifferentTypesOfParametersUsingHunts(Object parameter, Field expectedValue, String hint) {
        client.forSql("INSERT INTO tbl1(a) VALUES(?)", parameter)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactly(
                new SqlParameter()
                        .withName("1")
                        .withValue(expectedValue)
                        .withTypeHint(hint)
        );
    }

    @SuppressWarnings("RedundantCast")
    private static Stream<Arguments> typesThatNeedHints() {
        val now = LocalDateTime.now();
        return Stream.of(
                arguments(BigDecimal.valueOf(1.5), new Field().withStringValue("1.5"), "DECIMAL"),
                arguments(BigInteger.valueOf(15), new Field().withStringValue("15"), "DECIMAL"),
                arguments(now, new Field().withStringValue(DATE_TIME_FORMATTER.format(now)), "TIMESTAMP"),
                arguments(now.toLocalDate(), new Field().withStringValue(DATE_FORMATTER.format(now.toLocalDate())), "DATE"),
                arguments(now.toLocalTime(), new Field().withStringValue(TIME_FORMATTER.format(now.toLocalTime())), "TIME")
        );
    }

    @Test
    void shouldSupportNullParameter() {
        client.forSql("INSERT INTO tbl1(a) VALUES(?)", (Object) null)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactly(
                new SqlParameter()
                        .withName("1")
                        .withValue(new Field().withIsNull(true)));
    }

    @Test
    void shouldSupportNullVarargParameter() {
        client.forSql("INSERT INTO tbl1(a) VALUES(?)", (Object[]) null)
                .execute();

        val request = captureRequest();
        assertThat(request.getParameters()).containsExactly(
                new SqlParameter()
                        .withName("1")
                        .withValue(new Field().withIsNull(true)));
    }

    @Test
    void shouldThrowExceptionIfParameterIsOfUnknownType() {
        assertThatThrownBy(() -> client.forSql("INSERT INTO tbl1(a) VALUES(?)", new Object()).execute())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith(ERROR_PARAMETER_OF_UNKNOWN_TYPE);
    }
}
