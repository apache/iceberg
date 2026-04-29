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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.flink.sink.TaskWriterFactory;

/**
 * Pluggable provider that creates a {@link TaskWriterFactory} for a given write target. {@link
 * DynamicWriter} resolves the table, schema, partition spec, write properties and equality fields
 * before delegating final factory construction here, allowing callers to swap in a custom
 * implementation while keeping the surrounding validation and configuration logic intact.
 */
@FunctionalInterface
public interface DynamicTaskWriterFactoryProvider extends Serializable {

  TaskWriterFactory<RowData> create(
      Table table,
      RowType flinkSchema,
      long targetFileSizeBytes,
      FileFormat format,
      Map<String, String> writeProperties,
      List<Integer> equalityFieldIds,
      boolean upsertMode,
      Schema schema,
      PartitionSpec spec);

  DynamicTaskWriterFactoryProvider DEFAULT =
      (table,
          flinkSchema,
          targetFileSizeBytes,
          format,
          writeProperties,
          equalityFieldIds,
          upsertMode,
          schema,
          spec) ->
          new RowDataTaskWriterFactory(
              () -> table,
              flinkSchema,
              targetFileSizeBytes,
              format,
              writeProperties,
              equalityFieldIds,
              upsertMode,
              schema,
              spec);
}
