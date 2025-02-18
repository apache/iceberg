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

import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileAccessFactoryRegistry;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.spark.sql.catalyst.InternalRow;

abstract class BaseRowReader<T extends ScanTask> extends BaseReader<InternalRow, T> {
  BaseRowReader(
      Table table,
      ScanTaskGroup<T> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive) {
    super(table, taskGroup, tableSchema, expectedSchema, caseSensitive);
  }

  protected CloseableIterable<InternalRow> newIterable(
      InputFile file,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    ReadBuilder<?, InternalRow> reader =
        FileAccessFactoryRegistry.readBuilder(format, SparkObjectModels.SPARK_OBJECT_MODEL, file);
    return reader
        .project(projection)
        .constantFieldAccessors(idToConstant)
        .reuseContainers()
        .split(start, length)
        .filter(residual, caseSensitive())
        .nameMapping(nameMapping())
        .build();
  }
}
