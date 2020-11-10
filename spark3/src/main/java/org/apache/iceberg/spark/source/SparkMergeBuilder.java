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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.MergeBuilder;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkMergeBuilder implements MergeBuilder {

  private final SparkSession spark;
  private final Table table;
  private final LogicalWriteInfo writeInfo;
  private final IsolationLevel isolationLevel;

  // lazy vars
  private ScanBuilder lazyScanBuilder;
  private WriteBuilder lazyWriteBuilder;

  SparkMergeBuilder(SparkSession spark, Table table, LogicalWriteInfo writeInfo) {
    this.spark = spark;
    this.table = table;
    this.writeInfo = writeInfo;

    String isolationLevelAsString = table.properties().getOrDefault(
        TableProperties.WRITE_ISOLATION_LEVEL,
        TableProperties.WRITE_ISOLATION_LEVEL_DEFAULT
    ).toUpperCase(Locale.ROOT);
    this.isolationLevel = IsolationLevel.valueOf(isolationLevelAsString);
  }

  @Override
  public ScanBuilder asScanBuilder() {
    return scanBuilder();
  }

  private ScanBuilder scanBuilder() {
    if (lazyScanBuilder == null) {
      // ignore residuals to ensure we read full files
      SparkScanBuilder scanBuilder = new SparkScanBuilder(spark, table, scanOptions());
      lazyScanBuilder = scanBuilder.ignoreResiduals();
    }

    return lazyScanBuilder;
  }

  private CaseInsensitiveStringMap scanOptions() {
    Snapshot currentSnapshot = table.currentSnapshot();

    if (currentSnapshot == null) {
      return CaseInsensitiveStringMap.empty();
    }

    // set the snapshot id in the scan so that we can fetch it in the write
    Map<String, String> scanOptions = new HashMap<>();
    scanOptions.put("snapshot-id", String.valueOf(currentSnapshot.snapshotId()));
    return new CaseInsensitiveStringMap(scanOptions);
  }

  @Override
  public WriteBuilder asWriteBuilder() {
    return writeBuilder();
  }

  private WriteBuilder writeBuilder() {
    if (lazyWriteBuilder == null) {
      SparkWriteBuilder writeBuilder = new SparkWriteBuilder(spark, table, writeInfo);
      lazyWriteBuilder = writeBuilder.overwriteFiles(isolationLevel);
    }

    return lazyWriteBuilder;
  }
}
