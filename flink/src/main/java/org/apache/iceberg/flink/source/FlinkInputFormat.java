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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Flink {@link InputFormat} for Iceberg.
 */
public class FlinkInputFormat extends RichInputFormat<RowData, FlinkInputSplit> {

  private static final long serialVersionUID = 1L;

  private final TableLoader tableLoader;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final ScanContext context;
  private final RowDataFileScanTaskReader rowDataReader;
  private final int[][] schemaIndexes;
  private final int fieldLen;

  private transient DataIterator<RowData> iterator;
  private transient long currentReadCount = 0L;

  FlinkInputFormat(TableLoader tableLoader, Schema tableSchema, int[][] schemaIndexes, int fieldLen, FileIO io, EncryptionManager encryption,
                   ScanContext context) {
    this.tableLoader = tableLoader;
    this.io = io;
    this.encryption = encryption;
    this.context = context;
    this.schemaIndexes = schemaIndexes;
    this.fieldLen = fieldLen;
    this.rowDataReader = new RowDataFileScanTaskReader(tableSchema,
        context.project(), context.nameMapping(), context.caseSensitive());
  }

  @VisibleForTesting
  Schema projectedSchema() {
    return context.project();
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
    // Legacy method, not be used.
    return null;
  }

  @Override
  public FlinkInputSplit[] createInputSplits(int minNumSplits) throws IOException {
    // Called in Job manager, so it is OK to load table from catalog.
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      Table table = loader.loadTable();
      return FlinkSplitGenerator.createInputSplits(table, context);
    }
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(FlinkInputSplit[] inputSplits) {
    return new DefaultInputSplitAssigner(inputSplits);
  }

  @Override
  public void configure(Configuration parameters) {
  }

  @Override
  public void open(FlinkInputSplit split) {
    this.iterator = new DataIterator<>(rowDataReader, split.getTask(), io, encryption);
  }

  @Override
  public boolean reachedEnd() {
    if (context.limit() > 0 && currentReadCount >= context.limit()) {
      return true;
    } else {
      return !iterator.hasNext();
    }
  }

  @Override
  public RowData nextRecord(RowData reuse) {
    currentReadCount++;
    GenericRowData rowData = (GenericRowData)iterator.next();

    if (this.schemaIndexes == null) {
      return rowData;
    }
    GenericRowData flattenRowData = new GenericRowData(rowData.getRowKind(), fieldLen);

    for (int i = 0; i < this.schemaIndexes.length; i++) {
      flattenRowData.setField(i, extractRecord(rowData.getField(schemaIndexes[i][0]), schemaIndexes[i]));
    }

    return flattenRowData;
  }

  private Object extractRecord(Object record, int[] schemaIndex) {
    if (schemaIndex.length == 1) {
      return record;
    }
    Preconditions.checkArgument(record instanceof GenericRowData);
    GenericRowData value = (GenericRowData) record;

    for (int i = 1; i < schemaIndex.length; i++) {
      int index = schemaIndex[i];
      Object valueField = value.getField(index);
      if (i == schemaIndex.length - 1) {
        return valueField;
      }
      value = (GenericRowData)valueField;
    }

    return value;
  }

  @Override
  public void close() throws IOException {
    if (iterator != null) {
      iterator.close();
    }
  }
}
