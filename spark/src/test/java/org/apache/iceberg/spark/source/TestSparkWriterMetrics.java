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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.TestWriterMetrics;
import org.apache.iceberg.io.WriterFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

public class TestSparkWriterMetrics extends TestWriterMetrics<InternalRow> {

  public TestSparkWriterMetrics(FileFormat fileFormat) {
    super(fileFormat);
  }

  @Override
  protected WriterFactory<InternalRow> newWriterFactory(Schema dataSchema) {
    return SparkWriterFactory.builderFor(table)
        .dataSchema(table.schema())
        .dataFileFormat(fileFormat)
        .deleteFileFormat(fileFormat)
        .build();
  }

  @Override
  protected InternalRow toRow(Integer id, String data, boolean boolValue, Long longValue) {
    InternalRow row = new GenericInternalRow(3);
    row.update(0, id);
    row.update(1, UTF8String.fromString(data));

    InternalRow nested = new GenericInternalRow(2);
    nested.update(0, boolValue);
    nested.update(1, longValue);

    row.update(2, nested);
    return row;
  }
}
