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
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteConf;
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
    Preconditions.checkArgument(
        writeConf.rewrittenFileSetId() != null,
        "position_deletes table can only be written by RewriteDeleteFiles");

    // all files of rewrite group have same and partition and spec id
    ScanTaskSetManager scanTaskSetManager = ScanTaskSetManager.get();
    String fileSetId = writeConf.rewrittenFileSetId();
    List<PositionDeletesScanTask> scanTasks = scanTaskSetManager.fetchTasks(table, fileSetId);
    Preconditions.checkNotNull(scanTasks, "no scan tasks found for %s", fileSetId);

    Set<Integer> specIds =
        scanTasks.stream().map(t -> t.spec().specId()).collect(Collectors.toSet());
    Set<StructLike> partitions =
        scanTasks.stream().map(t -> t.file().partition()).collect(Collectors.toSet());
    Preconditions.checkArgument(
        specIds.size() == 1, "All scan tasks of %s are expected to have same spec id", fileSetId);
    Preconditions.checkArgument(
        partitions.size() == 1, "All scan tasks of %s are expected to have the same partition");
    int specId = scanTasks.get(0).spec().specId();
    StructLike partitionValue = scanTasks.get(0).partition();

    return new SparkPositionDeletesRewrite(
        spark, table, writeConf, writeInfo, writeSchema, dsSchema, specId, partitionValue);
  }
}
