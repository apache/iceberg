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

import static org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.DELETE;
import static org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.UPDATE;

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.iceberg.write.ExtendedLogicalWriteInfo;
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation;
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkCopyOnWriteOperation implements RowLevelOperation {

  private final SparkSession spark;
  private final Table table;
  private final Command command;
  private final IsolationLevel isolationLevel;

  // lazy vars
  private ScanBuilder lazyScanBuilder;
  private Scan configuredScan;
  private WriteBuilder lazyWriteBuilder;

  SparkCopyOnWriteOperation(
      SparkSession spark, Table table, RowLevelOperationInfo info, IsolationLevel isolationLevel) {
    this.spark = spark;
    this.table = table;
    this.command = info.command();
    this.isolationLevel = isolationLevel;
  }

  @Override
  public Command command() {
    return command;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    if (lazyScanBuilder == null) {
      lazyScanBuilder =
          new SparkScanBuilder(spark, table, options) {
            @Override
            public Scan build() {
              Scan scan = super.buildCopyOnWriteScan();
              SparkCopyOnWriteOperation.this.configuredScan = scan;
              return scan;
            }
          };
    }

    return lazyScanBuilder;
  }

  @Override
  public WriteBuilder newWriteBuilder(ExtendedLogicalWriteInfo info) {
    if (lazyWriteBuilder == null) {
      Preconditions.checkState(configuredScan != null, "Write must be configured after scan");
      SparkWriteBuilder writeBuilder = new SparkWriteBuilder(spark, table, info);
      lazyWriteBuilder = writeBuilder.overwriteFiles(configuredScan, command, isolationLevel);
    }

    return lazyWriteBuilder;
  }

  @Override
  public NamedReference[] requiredMetadataAttributes() {
    NamedReference file = Expressions.column(MetadataColumns.FILE_PATH.name());
    NamedReference pos = Expressions.column(MetadataColumns.ROW_POSITION.name());

    if (command == DELETE || command == UPDATE) {
      return new NamedReference[] {file, pos};
    } else {
      return new NamedReference[] {file};
    }
  }
}
