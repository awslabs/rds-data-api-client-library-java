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
    private Long numberOfRecordsUpdated;

    ExecutionResult(List<ColumnMetadata> metadata,
                    List<List<Field>> fields,
                    Long numberOfRecordsUpdated,
                    MappingOptions mappingOptions) {
        this.fieldNames = extractFieldNames(metadata, mappingOptions);
        this.rows = convertToRows(fields);
        this.numberOfRecordsUpdated = numberOfRecordsUpdated;
    }

    private List<String> extractFieldNames(List<ColumnMetadata> metadata, MappingOptions mappingOptions) {
        if (metadata == null) {
            return emptyList();
        }

        return metadata.stream()
                .map(entry -> getFieldName(entry, mappingOptions))
                .collect(toList());
    }

    private String getFieldName(ColumnMetadata columnMetadata, MappingOptions mappingOptions) {
        if (mappingOptions.useLabelForMapping)
            return columnMetadata.getLabel();
        return columnMetadata.getName();
    }

    private List<Row> convertToRows(List<List<Field>> records) {
        if (records == null) {
            return emptyList();
        }

        return records.stream()
                .map(Row::new)
                .collect(toList());
    }

    /**
     * Will return the number of records inserted/updated by the query.
     *
     * @return the number of records updated.
     */
    public Long getNumberOfRecordsUpdated() {
        return numberOfRecordsUpdated;
    }

    /**
     * Maps the first row from the result set retrieved from RDS Data API to the instance of the specified class.
     * If the result set is empty, a {@link MappingException} is thrown.
     * @param mapperClass class to map to
     * @return an instance of the specified class with the mapped data
     * @throws MappingException if failed to map RDS Data API results to the specified class
     */
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
                .orElseGet(() -> PropertyObjectWriter.create(mapperClass, fieldNames));
        return writer.write(row);
    }

    /**
     * Maps the result set retrieved from RDS Data API to the list of instances of the specified class.
     * @param mapperClass class to map to
     * @return a {@link List} of instances of the specified class with the mapped data
     * @throws MappingException if failed to map RDS Data API results to the specified class
     */
    public <T> List<T> mapToList(Class<T> mapperClass) {
        return rows.stream()
                .map(row -> mapToSingle(mapperClass, fieldNames, row))
                .collect(toList());
    }

    /**
     * Returns the single value from the first row and the first column from the result set, converting it to the type {@link T}
     * @param convertToType type to convert to
     * @return a value of a type {@link T} from the first row and the first column from the result set
     * @throws EmptyResultSetException if the result set is empty
     */
    public <T> T singleValue(Class<T> convertToType) {
        if (rows.size() == 0 || rows.get(0).columnCount() == 0)
            throw new EmptyResultSetException();

        return rows.get(0).getValue(0, convertToType);
    }

  @AllArgsConstructor
    static class Row {
        private List<Field> fields;

        public <T> T getValue(int index, Class<T> type) {
            return (T) TypeConverter.fromField(fields.get(index), type);
        }

        public int columnCount() {
            return fields.size();
        }
    }
}
