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

import com.amazonaws.services.rdsdata.model.Field;
import com.amazonaws.services.rdsdata.model.TypeHint;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static com.amazonaws.services.rdsdata.model.TypeHint.DATE;
import static com.amazonaws.services.rdsdata.model.TypeHint.DECIMAL;
import static com.amazonaws.services.rdsdata.model.TypeHint.TIME;
import static com.amazonaws.services.rdsdata.model.TypeHint.TIMESTAMP;

class TypeConverter {
    static String ERROR_PARAMETER_OF_UNKNOWN_TYPE = "Unknown parameter type: ";

    static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");
    static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss[.SSS]");
    static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    static Field toField(Object o) {
        if (o == null) {
            return new Field().withIsNull(true);
        } else if (o instanceof Byte || o instanceof Integer || o instanceof Long) {
            return new Field().withLongValue(((Number) o).longValue());
        } else if (o instanceof Double || o instanceof Float) {
            return new Field().withDoubleValue(((Number) o).doubleValue());
        } else if (o instanceof Character) {
            return new Field().withLongValue((long) (Character) o);
        } else if (o instanceof String) {
            return new Field().withStringValue(o.toString());
        } else if (o instanceof Boolean) {
            return new Field().withBooleanValue((Boolean) o);
        } else if (o instanceof byte[]) {
            return new Field().withBlobValue(ByteBuffer.wrap((byte[]) o));
        } else if (o instanceof BigDecimal || o instanceof BigInteger) {
            return new Field().withStringValue(o.toString());
        } else if (o instanceof LocalDateTime) {
            return new Field().withStringValue(DATE_TIME_FORMATTER.format((LocalDateTime) o));
        } else if (o instanceof LocalDate) {
            return new Field().withStringValue(DATE_FORMATTER.format((LocalDate) o));
        } else if (o instanceof LocalTime) {
            return new Field().withStringValue(TIME_FORMATTER.format((LocalTime) o));
        }

        throw new IllegalArgumentException(ERROR_PARAMETER_OF_UNKNOWN_TYPE + o.getClass().getName());
    }

    static Optional<TypeHint> getTypeHint(Object o) {
        if (o instanceof BigDecimal || o instanceof BigInteger) {
            return Optional.of(DECIMAL);
        } else if (o instanceof LocalDateTime) {
            return Optional.of(TIMESTAMP);
        } else if (o instanceof LocalDate) {
            return Optional.of(DATE);
        } else if (o instanceof LocalTime) {
            return Optional.of(TIME);
        }

        return Optional.empty();
    }

    static Object fromField(Field field, Class<?> type) {
        // TODO: Class comparison by == (or .equals) may not work if classes belong to different classloaders
        if (field.isNull() != null && field.isNull()) {
            return null;
        } if (type == String.class) {
            return field.getStringValue();
        } else if (type == Byte.class || type == byte.class) {
            return field.getLongValue().byteValue();
        } else if (type == Integer.class || type == int.class) {
            return field.getLongValue().intValue();
        } else if (type == Long.class || type == long.class) {
            return field.getLongValue();
        } else if (type == Character.class || type == char.class) {
            return (char) field.getLongValue().longValue();
        } else if (type == Double.class || type == double.class) {
            return field.getDoubleValue();
        } else if (type == Float.class || type == float.class) {
            return field.getDoubleValue().floatValue();
        } else if (type == byte[].class) {
            return field.getBlobValue().array();
        } else if (type == Boolean.class || type == boolean.class) {
            return field.getBooleanValue();
        } else if (type == BigDecimal.class) {
            return toBigDecimal(field);
        } else if (type == BigInteger.class) {
            return toBigInteger(field);
        }

        // TODO: handle this case
        return null;
    }

    private static BigDecimal toBigDecimal(Field field) {
        if (field.getStringValue() != null) {
            return new BigDecimal(field.getStringValue());
        } else if (field.getLongValue() != null) {
            return BigDecimal.valueOf(field.getLongValue());
        } else if (field.getDoubleValue() != null) {
            return BigDecimal.valueOf(field.getDoubleValue());
        }

        throw MappingException.cannotConvertToType(field, BigDecimal.class);
    }

    private static BigInteger toBigInteger(Field field) {
        if (field.getStringValue() != null) {
            return new BigInteger(field.getStringValue());
        } else if (field.getLongValue() != null) {
            return BigInteger.valueOf(field.getLongValue());
        }

        throw MappingException.cannotConvertToType(field, BigInteger.class);
    }
}
