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

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.rdsdata.model.Field;
import software.amazon.awssdk.services.rdsdata.model.TypeHint;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.UUID;

import static software.amazon.awssdk.services.rdsdata.model.TypeHint.DATE;
import static software.amazon.awssdk.services.rdsdata.model.TypeHint.DECIMAL;
import static software.amazon.awssdk.services.rdsdata.model.TypeHint.TIME;
import static software.amazon.awssdk.services.rdsdata.model.TypeHint.TIMESTAMP;

class TypeConverter {
    static String ERROR_PARAMETER_OF_UNKNOWN_TYPE = "Unknown parameter type: ";

    static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");
    static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss[.SSS]");
    static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    static Field toField(Object o) {
        if (o == null || o == FieldMapper.NULL) {
            return Field.builder().isNull(true).build();
        } else if (o instanceof Byte || o instanceof Integer || o instanceof Long) {
            return Field.builder().longValue(((Number) o).longValue()).build();
        } else if (o instanceof Double || o instanceof Float) {
            return Field.builder().doubleValue(((Number) o).doubleValue()).build();
        } else if (o instanceof Character) {
            return Field.builder().longValue((long) (Character) o).build();
        } else if (o instanceof String) {
            return Field.builder().stringValue(o.toString()).build();
        } else if (o instanceof Boolean) {
            return Field.builder().booleanValue((Boolean) o).build();
        } else if (o instanceof byte[]) {
            return Field.builder().blobValue(SdkBytes.fromByteArray((byte[]) o)).build();
        } else if (o instanceof BigDecimal || o instanceof BigInteger) {
            return Field.builder().stringValue(o.toString()).build();
        } else if (o instanceof LocalDateTime) {
            return Field.builder().stringValue(DATE_TIME_FORMATTER.format((LocalDateTime) o)).build();
        } else if (o instanceof LocalDate) {
            return Field.builder().stringValue(DATE_FORMATTER.format((LocalDate) o)).build();
        } else if (o instanceof LocalTime) {
            return Field.builder().stringValue(TIME_FORMATTER.format((LocalTime) o)).build();
        } else if (o instanceof Enum) {
            return Field.builder().stringValue(((Enum<?>) o).name()).build();
        } else if (o instanceof UUID) {
            return Field.builder().stringValue(o.toString()).build();
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
        } else if (o instanceof UUID) {
            return Optional.of(TypeHint.UUID);
        }

        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    static Object fromField(Field field, Class<?> type) {
        // TODO: Class comparison by == (or .equals) may not work if classes belong to different classloaders
        if (field.isNull() != null && field.isNull()) {
            return null;
        } if (type == String.class) {
            return field.stringValue();
        } else if (type == Byte.class || type == byte.class) {
            return field.longValue().byteValue();
        } else if (type == Integer.class || type == int.class) {
            return field.longValue().intValue();
        } else if (type == Long.class || type == long.class) {
            return field.longValue();
        } else if (type == Character.class || type == char.class) {
            return (char) field.longValue().longValue();
        } else if (type == Double.class || type == double.class) {
            return field.doubleValue();
        } else if (type == Float.class || type == float.class) {
            return field.doubleValue().floatValue();
        } else if (type == byte[].class) {
            return field.blobValue().asByteArray();
        } else if (type == Boolean.class || type == boolean.class) {
            return field.booleanValue();
        } else if (type == BigDecimal.class) {
            return toBigDecimal(field);
        } else if (type == BigInteger.class) {
            return toBigInteger(field);
        } else if (Enum.class.isAssignableFrom(type)) {
            return Enum.valueOf((Class<? extends Enum>) type, field.stringValue());
        } else if (type == UUID.class) {
            return java.util.UUID.fromString(field.stringValue());
        } else if (type == LocalDateTime.class) {
            return LocalDateTime.from(DATE_TIME_FORMATTER.parse(field.stringValue()));
        } else if (type == LocalDate.class) {
            return dateFromString(field.stringValue());
        } else if (type == LocalTime.class) {
            return timeFromString(field.stringValue());
        }

        // TODO: handle this case
        return null;
    }

    private static LocalDate dateFromString(String dateString) {
        try {
            // date can be provided in format "yyyy-MM-dd HH:mm:ss[.SSS]"
            return LocalDate.from(DATE_TIME_FORMATTER.parse(dateString));
        } catch (DateTimeParseException e) {
            // ... or as "yyyy-MM-dd"
            return LocalDate.from(DATE_FORMATTER.parse(dateString));
        }
    }

    private static LocalTime timeFromString(String timeString) {
        try {
            // time can be provided in format "yyyy-MM-dd HH:mm:ss[.SSS]"
            return LocalTime.from(DATE_TIME_FORMATTER.parse(timeString));
        } catch (DateTimeParseException e) {
            // ... or as "HH:mm:ss"
            return LocalTime.from(TIME_FORMATTER.parse(timeString));
        }
    }

    private static BigDecimal toBigDecimal(Field field) {
        if (field.stringValue() != null) {
            return new BigDecimal(field.stringValue());
        } else if (field.longValue() != null) {
            return BigDecimal.valueOf(field.longValue());
        } else if (field.doubleValue() != null) {
            return BigDecimal.valueOf(field.doubleValue());
        }

        throw MappingException.cannotConvertToType(field, BigDecimal.class);
    }

    private static BigInteger toBigInteger(Field field) {
        if (field.stringValue() != null) {
            return new BigInteger(field.stringValue());
        } else if (field.longValue() != null) {
            return BigInteger.valueOf(field.longValue());
        }

        throw MappingException.cannotConvertToType(field, BigInteger.class);
    }
}
