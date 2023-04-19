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
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals(
        ImmutableList.of(record.get(0), record.get(1)), soi.getStructFieldsDataAsList(record));

    StructField integerField = soi.getStructFieldRef("integer_field");
    Assert.assertEquals(record.get(0), soi.getStructFieldData(record, integerField));

    StructField structField = soi.getStructFieldRef("struct_field");
    Object innerData = soi.getStructFieldData(record, structField);
    Assert.assertEquals(innerRecord, innerData);

    StructObjectInspector innerSoi = (StructObjectInspector) structField.getFieldObjectInspector();
    StructField stringField = innerSoi.getStructFieldRef("string_field");

    Assert.assertEquals(
        ImmutableList.of(innerRecord.get(0)), innerSoi.getStructFieldsDataAsList(innerRecord));
    Assert.assertEquals(innerRecord.get(0), innerSoi.getStructFieldData(innerData, stringField));
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
    Assert.assertNull(soi.getStructFieldsDataAsList(null));
    StructField integerField = soi.getStructFieldRef("integer_field");
    Assert.assertNull(soi.getStructFieldData(null, integerField));
  }
}
