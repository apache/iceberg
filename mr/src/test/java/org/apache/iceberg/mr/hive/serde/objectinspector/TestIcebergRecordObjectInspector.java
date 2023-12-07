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
package org.apache.iceberg.mr.hive.serde.objectinspector;

import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergRecordObjectInspector {

  @Test
  public void testIcebergRecordObjectInspector() {
    Schema schema =
        new Schema(
            required(1, "integer_field", Types.IntegerType.get()),
            required(
                2,
                "struct_field",
                Types.StructType.of(
                    Types.NestedField.required(3, "string_field", Types.StringType.get()))));

    Record record = RandomGenericData.generate(schema, 1, 0L).get(0);
    Record innerRecord = record.get(1, Record.class);

    StructObjectInspector soi = (StructObjectInspector) IcebergObjectInspector.create(schema);
    Assertions.assertThat(soi.getStructFieldsDataAsList(record))
                    .isEqualTo(ImmutableList.of(record.get(0), record.get(1)));

    StructField integerField = soi.getStructFieldRef("integer_field");
    Assertions.assertThat(soi.getStructFieldData(record, integerField)).isEqualTo(record.get(0));

    StructField structField = soi.getStructFieldRef("struct_field");
    Object innerData = soi.getStructFieldData(record, structField);
    Assertions.assertThat(innerData).isEqualTo(innerRecord);

    StructObjectInspector innerSoi = (StructObjectInspector) structField.getFieldObjectInspector();
    StructField stringField = innerSoi.getStructFieldRef("string_field");

    Assertions.assertThat(innerSoi.getStructFieldsDataAsList(innerRecord))
            .isEqualTo(ImmutableList.of(innerRecord.get(0)));
    Assertions.assertThat(innerSoi.getStructFieldData(innerData, stringField)).isEqualTo(innerRecord.get(0));
  }

  @Test
  public void testIcebergRecordObjectInspectorWithRowNull() {
    Schema schema =
        new Schema(
            required(1, "integer_field", Types.IntegerType.get()),
            required(
                2,
                "struct_field",
                Types.StructType.of(
                    Types.NestedField.required(3, "string_field", Types.StringType.get()))));
    StructObjectInspector soi = (StructObjectInspector) IcebergObjectInspector.create(schema);
    Assertions.assertThat(soi.getStructFieldsDataAsList(null)).isNull();
    StructField integerField = soi.getStructFieldRef("integer_field");
    Assertions.assertThat(soi.getStructFieldData(null, integerField)).isNull();
  }
}
