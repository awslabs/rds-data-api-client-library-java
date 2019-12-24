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

import com.amazonaws.services.rdsdata.model.ColumnMetadata;
import com.amazonaws.services.rdsdata.model.Field;
import lombok.AllArgsConstructor;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class ExecutionResult {
    private List<String> fieldNames;
    private List<Row> rows;

    ExecutionResult(List<ColumnMetadata> metadata, List<List<Field>> fields) {
        this.fieldNames = extractFieldNames(metadata);
        this.rows = convertToRows(fields);
    }

    private List<String> extractFieldNames(List<ColumnMetadata> metadata) {
        if (metadata == null) {
            return emptyList();
        }

        return metadata.stream()
                .map(ColumnMetadata::getName)
                .collect(toList());
    }

    private List<Row> convertToRows(List<List<Field>> records) {
        if (records == null) {
            return emptyList();
        }

        return records.stream()
                .map(Row::new)
                .collect(toList());
    }

    public <T> T mapToSingle(Class<T> mapperClass) {
        if (rows.isEmpty()) {
            throw MappingException.emptyResultSet();
        }

        return mapToSingle(mapperClass, fieldNames, rows.get(0));
    }

    private <T> T mapToSingle(Class<T> mapperClass, List<String> fieldNames, Row row) {
        // TODO: check that columnMetadata array has the same length as fields

        // TODO: this can be cached
        ObjectWriter<T> writer = ConstructorObjectWriter.create(mapperClass, fieldNames)
                .orElseGet(() -> FieldObjectWriter.create(mapperClass, fieldNames));
        return writer.write(row);
    }

    public <T> List<T> mapToList(Class<T> mapperClass) {
        return rows.stream()
                .map(row -> mapToSingle(mapperClass, fieldNames, row))
                .collect(toList());
    }

    @AllArgsConstructor
    static class Row {
        private List<Field> fields;

        public <T> T getValue(int index, Class<T> type) {
            return (T) TypeConverter.fromField(fields.get(index), type);
        }
    }
}
