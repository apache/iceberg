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

import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A Spark Scan for a specific set of files determined in the constructor.
 */
public class SparkFileScan extends SparkMergeScan {

  // TODO probably swap this hierarchy where SparkMergeScan is a kind of SparkFileScan?

  private final List<FileScanTask> files;
  private final long splitSize;

  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  public static SparkFileScan scanOfFiles(SparkSession spark, Table table, List<FileScanTask> files, long splitSize) {
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    Broadcast<FileIO> io = jsc.broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryption = jsc.broadcast(table.encryption());

    return new SparkFileScan(
        table, io, encryption, false, false, table.schema(), ImmutableList.of(),
            CaseInsensitiveStringMap.empty(), files, splitSize);
  }

  SparkFileScan(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption,
                boolean caseSensitive, boolean ignoreResiduals, Schema expectedSchema,
                List<Expression> filters, CaseInsensitiveStringMap options, List<FileScanTask> files, long splitSize) {

    super(table, io, encryption, caseSensitive, ignoreResiduals, expectedSchema, filters, options);
    this.splitSize = splitSize;
    this.files = files;
  }

  @Override
  protected List<FileScanTask> files() {
    return files;
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files),
          splitSize);
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, splitSize, 1, 0);
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }
}
