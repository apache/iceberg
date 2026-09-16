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
package org.apache.iceberg.spark.source;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TestRollingFileWriters;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

public class TestSparkRollingFileWriters extends TestRollingFileWriters<InternalRow> {

  // Vortex has no Spark 4.2 integration yet -- spark/v4.2 has no iceberg-vortex
  // dependency and SparkFormatModels registers no Vortex model -- so it is excluded
  // from the parameter matrix inherited from the shared test base.
  @Parameters(name = "formatVersion = {0}, fileFormat = {1}, Partitioned = {2}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {2, FileFormat.AVRO, false},
        new Object[] {2, FileFormat.AVRO, true},
        new Object[] {2, FileFormat.PARQUET, false},
        new Object[] {2, FileFormat.PARQUET, true},
        new Object[] {2, FileFormat.ORC, false},
        new Object[] {2, FileFormat.ORC, true});
  }

  @Override
  protected FileWriterFactory<InternalRow> newWriterFactory(
      Schema dataSchema,
      List<Integer> equalityFieldIds,
      Schema equalityDeleteRowSchema,
      Schema positionDeleteRowSchema) {
    return SparkFileWriterFactory.builderFor(table)
        .dataSchema(table.schema())
        .dataFileFormat(format())
        .deleteFileFormat(format())
        .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
        .equalityDeleteRowSchema(equalityDeleteRowSchema)
        .build();
  }

  @Override
  protected InternalRow toRow(Integer id, String data) {
    InternalRow row = new GenericInternalRow(2);
    row.update(0, id);
    row.update(1, UTF8String.fromString(data));
    return row;
  }
}
