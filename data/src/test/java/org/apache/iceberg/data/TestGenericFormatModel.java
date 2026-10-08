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
package org.apache.iceberg.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * File Format API TCK binding for the generic {@link Record} object model.
 *
 * <p>Unlike the Spark and Flink bindings, the generic engine represents every Iceberg type
 * directly, so it exercises the full generator matrix (including geospatial types) against the
 * Avro, ORC and Parquet object models. Format-level gaps (for example ORC's lack of geospatial
 * support) are handled by {@code MISSING_FEATURES}/{@code supportsGenerator} in the base class.
 */
public class TestGenericFormatModel extends BaseFormatModelTests<Record> {

  @Override
  protected Class<Record> engineType() {
    return Record.class;
  }

  @Override
  protected Object engineSchema(Schema schema) {
    // The generic object model derives its schema from the Iceberg schema; no engine-native
    // schema is required.
    return null;
  }

  @Override
  protected Record convertToEngine(Record record, Schema schema) {
    return record;
  }

  @Override
  protected void assertEquals(Schema schema, List<Record> expected, List<Record> actual) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      DataTestHelpers.assertEquals(
          schema.asStruct(), expected.get(i), asRecord(schema.asStruct(), actual.get(i)));
    }
  }

  @Override
  protected Object convertConstantToEngine(Type type, Object value) {
    return value;
  }

  /**
   * Normalizes a read struct into a {@link Record}. Some object models materialize nested structs
   * (notably the {@code _partition} metadata column) as a non-{@code Record} {@link StructLike}
   * such as {@code PartitionData}; this rebuilds them as {@link GenericRecord} so the generic
   * comparison in {@link DataTestHelpers} accepts them.
   */
  private static Record asRecord(Types.StructType struct, StructLike row) {
    GenericRecord record = GenericRecord.create(struct);
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i++) {
      Type fieldType = fields.get(i).type();
      Object value = row.get(i, Object.class);
      if (value instanceof StructLike && fieldType.isStructType()) {
        value = asRecord(fieldType.asStructType(), (StructLike) value);
      }

      record.set(i, value);
    }

    return record;
  }
}
