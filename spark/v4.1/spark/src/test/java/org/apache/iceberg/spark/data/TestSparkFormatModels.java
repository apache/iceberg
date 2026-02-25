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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.TestBaseFormatModel;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

public class TestSparkFormatModels extends TestBaseFormatModel<InternalRow, InternalRow> {

  @Override
  protected Class<InternalRow> writeType() {
    return InternalRow.class;
  }

  @Override
  protected Class<InternalRow> readType() {
    return InternalRow.class;
  }

  @Override
  protected Object writeEngineSchema(Schema schema) {
    return SparkSchemaUtil.convert(schema);
  }

  @Override
  protected Object readEngineSchema(Schema schema) {
    return SparkSchemaUtil.convert(schema);
  }

  @Override
  protected List<InternalRow> testRecords() {
    return Lists.newArrayList(RandomData.generateSpark(TestBase.SCHEMA, 10, 1L));
  }

  @Override
  protected void assertEquals(
      Types.StructType struct, List<InternalRow> expected, List<InternalRow> actual) {
    assertThat(actual).hasSameSizeAs(expected);
    Schema schema = new Schema(struct.fields());
    for (int i = 0; i < expected.size(); i++) {
      TestHelpers.assertEquals(schema, expected.get(i), actual.get(i));
    }
  }

  @Override
  protected List<InternalRow> expectedPositionDeletes(Schema schema) {
    return ImmutableList.of(
        toPositionDeleteRow("data-file-1.parquet", 0L),
        toPositionDeleteRow("data-file-1.parquet", 1L));
  }

  private static InternalRow toPositionDeleteRow(String filePath, long pos) {
    InternalRow row = new GenericInternalRow(2);
    row.update(0, UTF8String.fromString(filePath));
    row.update(1, pos);
    return row;
  }
}
