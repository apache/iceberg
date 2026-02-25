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

import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.TestBaseFormatModel;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;

public class TestSparkReadModel extends TestBaseFormatModel<Record, InternalRow> {

  @Override
  protected Class<Record> writeType() {
    return Record.class;
  }

  @Override
  protected Class<InternalRow> readType() {
    return InternalRow.class;
  }

  @Override
  protected Object writeEngineSchema(Schema schema) {
    return null;
  }

  @Override
  protected Object readEngineSchema(Schema schema) {
    return SparkSchemaUtil.convert(schema);
  }

  @Override
  protected List<Record> testRecords() {
    return RandomGenericData.generate(TestBase.SCHEMA, 10, 1L);
  }

  @Override
  protected void assertEquals(
      Types.StructType struct, List<Record> expected, List<InternalRow> actual) {
    for (int i = 0; i < expected.size(); i++) {
      GenericsHelpers.assertEqualsUnsafe(struct, expected.get(i), actual.get(i));
    }
  }

  @Override
  protected List<Record> expectedPositionDeletes(Schema schema) {
    return ImmutableList.of(
        GenericRecord.create(schema)
            .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 0L),
        GenericRecord.create(schema)
            .copy(DELETE_FILE_PATH.name(), "data-file-1.parquet", DELETE_FILE_POS.name(), 1L));
  }
}
