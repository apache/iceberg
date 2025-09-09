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
package org.apache.iceberg.flink.maintenance.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.ScanContext;

/** A specialized reader implementation that extracts file names from Iceberg table rows. */
@Internal
public class FileNameReader extends TableReader<String> {

  public FileNameReader(
      String taskName,
      int taskIndex,
      TableLoader tableLoader,
      Schema projectedSchema,
      ScanContext scanContext,
      MetadataTableType metadataTableType) {
    super(taskName, taskIndex, tableLoader, projectedSchema, scanContext, metadataTableType);
  }

  @Override
  void extract(RowData rowData, Collector<String> out) {
    if (rowData != null && rowData.getString(0) != null) {
      out.collect(rowData.getString(0).toString());
    }
  }
}
