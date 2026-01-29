/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;

/**
 * Base test class for Delta Writer tests with CDC (Change Data Capture) support.
 *
 * <p>Provides CDC-specific schemas with configurable CDC field locations and helper methods for
 * creating CDC records and validating test results.
 */
public abstract class DeltaWriterTestBase extends WriterTestBase {

  protected static final Schema CDC_SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get()),
              Types.NestedField.required(4, "_op", Types.StringType.get())),
          ImmutableSet.of(1, 3));

  protected static final Schema CDC_SCHEMA_NESTED =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              Types.NestedField.required(3, "id2", Types.LongType.get()),
              Types.NestedField.required(
                  4,
                  "_cdc",
                  Types.StructType.of(
                      Types.NestedField.required(5, "op", Types.StringType.get())))),
          ImmutableSet.of(1, 3));

  /**
   * Creates a CDC record with the given values using a flat CDC field location (e.g., "_op").
   *
   * @param id the id field value
   * @param data the data field value
   * @param id2 the id2 field value (second identifier)
   * @param op the CDC operation (C/R for INSERT, U for UPDATE, D for DELETE)
   * @return a CDC record with all fields set
   */
  protected Record createCDCRecord(long id, String data, long id2, String op) {
    return createCDCRecord(id, data, id2, op, CDC_SCHEMA, "_op");
  }

  /**
   * Creates a CDC record with the given values and CDC field location.
   *
   * @param id the id field value
   * @param data the data field value
   * @param id2 the id2 field value (second identifier)
   * @param op the CDC operation (C/R for INSERT, U for UPDATE, D for DELETE)
   * @param schema the schema to use for record creation
   * @param cdcFieldPath the path to the CDC field (e.g., "_op" or "_cdc.op")
   * @return a CDC record with all fields set
   */
  protected Record createCDCRecord(
      long id, String data, long id2, String op, Schema schema, String cdcFieldPath) {
    Record record = GenericRecord.create(schema);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("id2", id2);
    setNestedField(record, cdcFieldPath, op);
    return record;
  }

  /**
   * Creates a CDC record with matching id and id2 values.
   *
   * @param id the id field value (also used for id2)
   * @param data the data field value
   * @param op the CDC operation (C/R for INSERT, U for UPDATE, D for DELETE)
   * @return a CDC record with all fields set
   */
  protected Record createCDCRecord(long id, String data, String op) {
    return createCDCRecord(id, data, id, op);
  }

  /**
   * Creates a CDC record with matching id and id2 values using a custom schema and CDC field
   * location.
   *
   * @param id the id field value (also used for id2)
   * @param data the data field value
   * @param schema the schema to use for record creation
   * @param cdcFieldPath the path to the CDC field (e.g., "_op" or "_cdc.op")
   * @param op the CDC operation (C/R for INSERT, U for UPDATE, D for DELETE)
   * @return a CDC record with all fields set
   */
  protected Record createCDCRecord(
      long id, String data, Schema schema, String cdcFieldPath, String op) {
    return createCDCRecord(id, data, id, op, schema, cdcFieldPath);
  }

  /**
   * Sets a field value, supporting nested paths like "_cdc.op".
   *
   * @param record the record to set the field on
   * @param fieldPath the field path (e.g., "_op" or "_cdc.op")
   * @param value the value to set
   */
  private void setNestedField(Record record, String fieldPath, String value) {
    String[] parts = fieldPath.split("\\.");
    if (parts.length == 1) {
      record.setField(fieldPath, value);
    } else {
      Record nested = (Record) record.getField(parts[0]);
      if (nested == null) {
        // Create the nested record from the schema's struct type
        Types.NestedField nestedField = record.struct().field(parts[0]);
        if (nestedField == null) {
          throw new IllegalStateException("Nested field " + parts[0] + " not found in schema");
        }
        Types.StructType structType = nestedField.type().asStructType();
        nested = GenericRecord.create(structType);
        record.setField(parts[0], nested);
      }
      nested.setField(parts[1], value);
    }
  }

  /**
   * Validates that the actual data matches the expected data, including the CDC field.
   *
   * <p>Note: This method validates by comparing record fields. For InMemoryFileIO-based tests, this
   * is primarily useful for validating WriteResult contents.
   *
   * @param actual the actual records
   * @param expected the expected records
   */
  protected void assertDataMatches(List<Record> actual, List<Record> expected) {
    assertDataMatches(actual, expected, "_op");
  }

  /**
   * Validates that the actual data matches the expected data, including the CDC field.
   *
   * <p>Note: This method validates by comparing record fields. For InMemoryFileIO-based tests, this
   * is primarily useful for validating WriteResult contents.
   *
   * @param actual the actual records
   * @param expected the expected records
   * @param cdcFieldPath the path to the CDC field (e.g., "_op" or "_cdc.op")
   */
  protected void assertDataMatches(
      List<Record> actual, List<Record> expected, String cdcFieldPath) {
    assertThat(actual).hasSize(expected.size());

    for (int i = 0; i < expected.size(); i++) {
      Record expectedRecord = expected.get(i);
      Record actualRecord = actual.get(i);

      assertThat(actualRecord.getField("id")).isEqualTo(expectedRecord.getField("id"));
      assertThat(actualRecord.getField("data")).isEqualTo(expectedRecord.getField("data"));
      assertThat(actualRecord.getField("id2")).isEqualTo(expectedRecord.getField("id2"));
      assertThat(getNestedField(actualRecord, cdcFieldPath))
          .isEqualTo(getNestedField(expectedRecord, cdcFieldPath));
    }
  }

  /**
   * Validates that the actual data matches the expected data, ignoring the _op field.
   *
   * <p>Useful for validating the final data state after CDC operations have been applied, where the
   * _op field is not relevant to the final result.
   *
   * @param actual the actual records
   * @param expected the expected records (can use any _op value)
   */
  protected void assertDataMatchesIgnoringOp(List<Record> actual, List<Record> expected) {
    assertThat(actual).hasSize(expected.size());

    for (int i = 0; i < expected.size(); i++) {
      Record expectedRecord = expected.get(i);
      Record actualRecord = actual.get(i);

      assertThat(actualRecord.getField("id")).isEqualTo(expectedRecord.getField("id"));
      assertThat(actualRecord.getField("data")).isEqualTo(expectedRecord.getField("data"));
      assertThat(actualRecord.getField("id2")).isEqualTo(expectedRecord.getField("id2"));
    }
  }

  /**
   * Helper method to validate that a record has the expected field values.
   *
   * @param record the record to validate
   * @param expectedId the expected id value
   * @param expectedData the expected data value
   * @param expectedId2 the expected id2 value
   * @param expectedOp the expected CDC operation value
   */
  protected void assertRecordEquals(
      Record record, long expectedId, String expectedData, long expectedId2, String expectedOp) {
    assertRecordEquals(record, expectedId, expectedData, expectedId2, expectedOp, "_op");
  }

  /**
   * Helper method to validate that a record has the expected field values.
   *
   * @param record the record to validate
   * @param expectedId the expected id value
   * @param expectedData the expected data value
   * @param expectedId2 the expected id2 value
   * @param expectedOp the expected CDC operation value
   * @param cdcFieldPath the path to the CDC field (e.g., "_op" or "_cdc.op")
   */
  protected void assertRecordEquals(
      Record record,
      long expectedId,
      String expectedData,
      long expectedId2,
      String expectedOp,
      String cdcFieldPath) {
    assertThat(record.getField("id")).isEqualTo(expectedId);
    assertThat(record.getField("data")).isEqualTo(expectedData);
    assertThat(record.getField("id2")).isEqualTo(expectedId2);
    assertThat(getNestedField(record, cdcFieldPath)).isEqualTo(expectedOp);
  }

  /**
   * Gets a field value, supporting nested paths like "_cdc.op".
   *
   * @param record the record to get the field from
   * @param fieldPath the field path (e.g., "_op" or "_cdc.op")
   * @return the field value
   */
  private String getNestedField(Record record, String fieldPath) {
    String[] parts = fieldPath.split("\\.");
    if (parts.length == 1) {
      return (String) record.getField(fieldPath);
    } else {
      Record nested = (Record) record.getField(parts[0]);
      if (nested == null) {
        throw new IllegalStateException("Nested field " + parts[0] + " not found");
      }
      return (String) nested.getField(parts[1]);
    }
  }

  /**
   * Helper method to read all data files from a WriteResult and return their contents.
   *
   * <p>Note: This is a placeholder for future implementation. Reading data back from InMemoryFileIO
   * requires setting up Iceberg readers which may be complex.
   *
   * @param result the WriteResult to read from
   * @return list of records read from data files
   * @throws IOException if reading fails
   */
  protected List<Record> readDataFiles(org.apache.iceberg.io.WriteResult result)
      throws IOException {
    // TODO: Implement if needed for data validation
    // This would require:
    // 1. Creating a FileReaderFactory
    // 2. Reading each DataFile from result.dataFiles()
    // 3. Collecting all records into a list
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
