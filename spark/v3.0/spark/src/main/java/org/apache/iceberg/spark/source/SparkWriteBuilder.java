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

import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

class SparkWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {

  private final SparkSession spark;
  private final Table table;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo writeInfo;
  private final StructType dsSchema;
  private final String overwriteMode;
  private final boolean handleTimestampWithoutZone;
  private boolean overwriteDynamic = false;
  private boolean overwriteByFilter = false;
  private Expression overwriteExpr = null;
  private boolean overwriteFiles = false;
  private SparkMergeScan mergeScan = null;
  private IsolationLevel isolationLevel = null;

  SparkWriteBuilder(SparkSession spark, Table table, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.writeConf = new SparkWriteConf(spark, table, info.options());
    this.writeInfo = info;
    this.dsSchema = info.schema();
    this.overwriteMode = writeConf.overwriteMode();
    this.handleTimestampWithoutZone = writeConf.handleTimestampWithoutZone();
  }

  public WriteBuilder overwriteFiles(Scan scan, IsolationLevel writeIsolationLevel) {
    Preconditions.checkArgument(scan instanceof SparkMergeScan, "%s is not SparkMergeScan", scan);
    Preconditions.checkState(!overwriteByFilter, "Cannot overwrite individual files and by filter");
    Preconditions.checkState(
        !overwriteDynamic, "Cannot overwrite individual files and dynamically");
    this.overwriteFiles = true;
    this.mergeScan = (SparkMergeScan) scan;
    this.isolationLevel = writeIsolationLevel;
    return this;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    Preconditions.checkState(
        !overwriteByFilter, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
    Preconditions.checkState(!overwriteFiles, "Cannot overwrite individual files and dynamically");
    this.overwriteDynamic = true;
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    Preconditions.checkState(
        !overwriteFiles, "Cannot overwrite individual files and using filters");
    this.overwriteExpr = SparkFilters.convert(filters);
    if (overwriteExpr == Expressions.alwaysTrue() && "dynamic".equals(overwriteMode)) {
      // use the write option to override truncating the table. use dynamic overwrite instead.
      this.overwriteDynamic = true;
    } else {
      Preconditions.checkState(
          !overwriteDynamic, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
      this.overwriteByFilter = true;
    }
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    // Validate
    Preconditions.checkArgument(
        handleTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(table.schema()),
        SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);

    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema);
    TypeUtil.validateWriteSchema(
        table.schema(), writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());
    SparkUtil.validatePartitionTransforms(table.spec());

    // Get application id
    String appId = spark.sparkContext().applicationId();

    SparkWrite write =
        new SparkWrite(spark, table, writeConf, writeInfo, appId, writeSchema, dsSchema);
    if (overwriteByFilter) {
      return write.asOverwriteByFilter(overwriteExpr);
    } else if (overwriteDynamic) {
      return write.asDynamicOverwrite();
    } else if (overwriteFiles) {
      return write.asCopyOnWriteMergeWrite(mergeScan, isolationLevel);
    } else {
      return write.asBatchAppend();
    }
  }

  @Override
  public StreamingWrite buildForStreaming() {
    // Validate
    Preconditions.checkArgument(
        handleTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(table.schema()),
        SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);

    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema);
    TypeUtil.validateWriteSchema(
        table.schema(), writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());
    SparkUtil.validatePartitionTransforms(table.spec());

    // Change to streaming write if it is just append
    Preconditions.checkState(
        !overwriteDynamic, "Unsupported streaming operation: dynamic partition overwrite");
    Preconditions.checkState(
        !overwriteByFilter || overwriteExpr == Expressions.alwaysTrue(),
        "Unsupported streaming operation: overwrite by filter: %s",
        overwriteExpr);

    // Get application id
    String appId = spark.sparkContext().applicationId();

    SparkWrite write =
        new SparkWrite(spark, table, writeConf, writeInfo, appId, writeSchema, dsSchema);
    if (overwriteByFilter) {
      return write.asStreamingOverwrite();
    } else {
      return write.asStreamingAppend();
    }
  }
}
