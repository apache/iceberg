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

import java.util.Locale;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class SparkWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {

  private final SparkSession spark;
  private final Table table;
  private final String writeQueryId;
  private final StructType dsSchema;
  private final CaseInsensitiveStringMap options;
  private final String overwriteMode;
  private boolean overwriteDynamic = false;
  private boolean overwriteByFilter = false;
  private Expression overwriteExpr = null;

  // lazy variables
  private JavaSparkContext lazySparkContext = null;

  SparkWriteBuilder(SparkSession spark, Table table, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.writeQueryId = info.queryId();
    this.dsSchema = info.schema();
    this.options = info.options();
    this.overwriteMode = options.containsKey("overwrite-mode") ?
        options.get("overwrite-mode").toLowerCase(Locale.ROOT) : null;
  }

  private JavaSparkContext lazySparkContext() {
    if (lazySparkContext == null) {
      this.lazySparkContext = new JavaSparkContext(spark.sparkContext());
    }
    return lazySparkContext;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    Preconditions.checkState(!overwriteByFilter, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
    this.overwriteDynamic = true;
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    this.overwriteExpr = SparkFilters.convert(filters);
    if (overwriteExpr == Expressions.alwaysTrue() && "dynamic".equals(overwriteMode)) {
      // use the write option to override truncating the table. use dynamic overwrite instead.
      this.overwriteDynamic = true;
    } else {
      Preconditions.checkState(!overwriteDynamic, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
      this.overwriteByFilter = true;
    }
    return this;
  }

  @Override
  public BatchWrite buildForBatch() {
    // Validate
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema);
    TypeUtil.validateWriteSchema(table.schema(), writeSchema,
        checkNullability(spark, options), checkOrdering(spark, options));
    SparkUtil.validatePartitionTransforms(table.spec());

    // Get application id
    String appId = spark.sparkContext().applicationId();

    // Get write-audit-publish id
    String wapId = spark.conf().get("spark.wap.id", null);

    Broadcast<FileIO> io = lazySparkContext().broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryptionManager = lazySparkContext().broadcast(table.encryption());

    if (overwriteByFilter) {
      return new OverwriteByFilterBatchWrite(
          table, io, encryptionManager, options, appId, wapId,
          writeSchema, dsSchema, overwriteExpr);
    } else if (overwriteDynamic) {
      return new OverwriteDynamicBatchWrite(
          table, io, encryptionManager, options, appId, wapId,
          writeSchema, dsSchema);
    } else {
      return new AppendBatchWrite(
          table, io, encryptionManager, options, appId, wapId,
          writeSchema, dsSchema);
    }
  }

  @Override
  public StreamingWrite buildForStreaming() {
    // Validate
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema);
    TypeUtil.validateWriteSchema(table.schema(), writeSchema,
        checkNullability(spark, options), checkOrdering(spark, options));
    SparkUtil.validatePartitionTransforms(table.spec());

    // Change to streaming write if it is just append
    Preconditions.checkState(!overwriteDynamic,
        "Unsupported streaming operation: dynamic partition overwrite");
    Preconditions.checkState(!overwriteByFilter || overwriteExpr == Expressions.alwaysTrue(),
        "Unsupported streaming operation: overwrite by filter: %s", overwriteExpr);

    // Get application id
    String appId = spark.sparkContext().applicationId();

    // Get write-audit-publish id
    String wapId = spark.conf().get("spark.wap.id", null);

    Broadcast<FileIO> io = lazySparkContext().broadcast(SparkUtil.serializableFileIO(table));
    Broadcast<EncryptionManager> encryptionManager = lazySparkContext().broadcast(table.encryption());

    return new SparkStreamingWrite(
        table, io, encryptionManager, options, overwriteByFilter, writeQueryId, appId, wapId, writeSchema, dsSchema);
  }

  private static boolean checkNullability(SparkSession spark, CaseInsensitiveStringMap options) {
    boolean sparkCheckNullability = Boolean.parseBoolean(
        spark.conf().get("spark.sql.iceberg.check-nullability", "true"));
    boolean dataFrameCheckNullability = options.getBoolean("check-nullability", true);
    return sparkCheckNullability && dataFrameCheckNullability;
  }

  private static boolean checkOrdering(SparkSession spark, CaseInsensitiveStringMap options) {
    boolean sparkCheckOrdering = Boolean.parseBoolean(spark.conf()
        .get("spark.sql.iceberg.check-ordering", "true"));
    boolean dataFrameCheckOrdering = options.getBoolean("check-ordering", true);
    return sparkCheckOrdering && dataFrameCheckOrdering;
  }
}
