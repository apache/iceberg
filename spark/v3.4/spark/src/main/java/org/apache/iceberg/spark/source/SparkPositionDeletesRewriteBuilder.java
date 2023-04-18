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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * Builder class for rewrites of position delete files from Spark. Responsible for creating {@link
 * SparkPositionDeletesRewrite}.
 *
 * <p>This class is meant to be used for an action to rewrite delete files. Hence, it makes an
 * assumption that all incoming deletes belong to the same partition, and that incoming dataset is
 * from {@link ScanTaskSetManager}.
 */
public class SparkPositionDeletesRewriteBuilder implements WriteBuilder {

  private final SparkSession spark;
  private final Table table;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo writeInfo;
  private final StructType dsSchema;
  private final Schema writeSchema;

  SparkPositionDeletesRewriteBuilder(
      SparkSession spark, Table table, String branch, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.writeConf = new SparkWriteConf(spark, table, branch, info.options());
    this.writeInfo = info;
    this.dsSchema = info.schema();
    this.writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema, writeConf.caseSensitive());
  }

  @Override
  public Write build() {
    String fileSetId = writeConf.rewrittenFileSetId();
    boolean handleTimestampWithoutZone = writeConf.handleTimestampWithoutZone();

    Preconditions.checkArgument(
        fileSetId != null, "Can only write to %s via actions", table.name());
    Preconditions.checkArgument(
        handleTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(table.schema()),
        SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);

    // all files of rewrite group have same partition and spec id
    ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
    List<PositionDeletesScanTask> tasks = taskSetManager.fetchTasks(table, fileSetId);
    Preconditions.checkArgument(
        tasks != null && tasks.size() > 0, "No scan tasks found for %s", fileSetId);

    int specId = specId(fileSetId, tasks);
    StructLike partition = partition(fileSetId, tasks);

    return new SparkPositionDeletesRewrite(
        spark, table, writeConf, writeInfo, writeSchema, dsSchema, specId, partition);
  }

  private int specId(String fileSetId, List<PositionDeletesScanTask> tasks) {
    Set<Integer> specIds = tasks.stream().map(t -> t.spec().specId()).collect(Collectors.toSet());
    Preconditions.checkArgument(
        specIds.size() == 1,
        "All scan tasks of %s are expected to have same spec id, but got %s",
        fileSetId,
        Joiner.on(",").join(specIds));
    return tasks.get(0).spec().specId();
  }

  private StructLike partition(String fileSetId, List<PositionDeletesScanTask> tasks) {
    StructLikeSet partitions = StructLikeSet.create(tasks.get(0).spec().partitionType());
    tasks.stream().map(ContentScanTask::partition).forEach(partitions::add);
    Preconditions.checkArgument(
        partitions.size() == 1,
        "All scan tasks of %s are expected to have the same partition, but got %s",
        fileSetId,
        Joiner.on(",").join(partitions));
    return tasks.get(0).partition();
  }
}
