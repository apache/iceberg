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

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {
  private static final Logger LOG = LoggerFactory.getLogger(SparkWriteBuilder.class);
  private static final SortOrder[] NO_ORDERING = new SortOrder[0];

  private final SparkSession spark;
  private final Table table;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo writeInfo;
  private final StructType dsSchema;
  private final String overwriteMode;
  private final String rewrittenFileSetId;
  private final boolean useTableDistributionAndOrdering;
  private boolean overwriteDynamic = false;
  private boolean overwriteByFilter = false;
  private Expression overwriteExpr = null;
  private boolean overwriteFiles = false;
  private SparkCopyOnWriteScan copyOnWriteScan = null;
  private Command copyOnWriteCommand = null;
  private IsolationLevel copyOnWriteIsolationLevel = null;

  SparkWriteBuilder(SparkSession spark, Table table, String branch, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.writeConf = new SparkWriteConf(spark, table, branch, info.options());
    this.writeInfo = info;
    this.dsSchema = info.schema();
    this.overwriteMode = writeConf.overwriteMode();
    this.rewrittenFileSetId = writeConf.rewrittenFileSetId();
    this.useTableDistributionAndOrdering = writeConf.useTableDistributionAndOrdering();
  }

  public WriteBuilder overwriteFiles(Scan scan, Command command, IsolationLevel isolationLevel) {
    Preconditions.checkState(!overwriteByFilter, "Cannot overwrite individual files and by filter");
    Preconditions.checkState(
        !overwriteDynamic, "Cannot overwrite individual files and dynamically");
    Preconditions.checkState(
        rewrittenFileSetId == null, "Cannot overwrite individual files and rewrite");

    this.overwriteFiles = true;
    this.copyOnWriteScan = (SparkCopyOnWriteScan) scan;
    this.copyOnWriteCommand = command;
    this.copyOnWriteIsolationLevel = isolationLevel;
    return this;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    Preconditions.checkState(
        !overwriteByFilter, "Cannot overwrite dynamically and by filter: %s", overwriteExpr);
    Preconditions.checkState(!overwriteFiles, "Cannot overwrite individual files and dynamically");
    Preconditions.checkState(
        rewrittenFileSetId == null, "Cannot overwrite dynamically and rewrite");

    this.overwriteDynamic = true;
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    Preconditions.checkState(
        !overwriteFiles, "Cannot overwrite individual files and using filters");
    Preconditions.checkState(rewrittenFileSetId == null, "Cannot overwrite and rewrite");

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
  public Write build() {
    // Validate
    Schema writeSchema = validateOrMergeWriteSchema(table, dsSchema, writeConf);
    SparkUtil.validatePartitionTransforms(table.spec());

    // Get application id
    String appId = spark.sparkContext().applicationId();

    Distribution distribution;
    SortOrder[] ordering;

    if (useTableDistributionAndOrdering) {
      distribution = buildRequiredDistribution();
      ordering = buildRequiredOrdering(distribution);
    } else {
      LOG.info("Skipping distribution/ordering: disabled per job configuration");
      distribution = Distributions.unspecified();
      ordering = NO_ORDERING;
    }

    return new SparkWrite(
        spark, table, writeConf, writeInfo, appId, writeSchema, dsSchema, distribution, ordering) {

      @Override
      public BatchWrite toBatch() {
        if (rewrittenFileSetId != null) {
          return asRewrite(rewrittenFileSetId);
        } else if (overwriteByFilter) {
          return asOverwriteByFilter(overwriteExpr);
        } else if (overwriteDynamic) {
          return asDynamicOverwrite();
        } else if (overwriteFiles) {
          return asCopyOnWriteOperation(copyOnWriteScan, copyOnWriteIsolationLevel);
        } else {
          return asBatchAppend();
        }
      }

      @Override
      public StreamingWrite toStreaming() {
        Preconditions.checkState(
            !overwriteDynamic, "Unsupported streaming operation: dynamic partition overwrite");
        Preconditions.checkState(
            !overwriteByFilter || overwriteExpr == Expressions.alwaysTrue(),
            "Unsupported streaming operation: overwrite by filter: %s",
            overwriteExpr);
        Preconditions.checkState(
            rewrittenFileSetId == null, "Unsupported streaming operation: rewrite");

        if (overwriteByFilter) {
          return asStreamingOverwrite();
        } else {
          return asStreamingAppend();
        }
      }
    };
  }

  private Distribution buildRequiredDistribution() {
    if (overwriteFiles) {
      DistributionMode distributionMode = copyOnWriteDistributionMode();
      return SparkDistributionAndOrderingUtil.buildCopyOnWriteDistribution(
          table, copyOnWriteCommand, distributionMode);
    } else {
      DistributionMode distributionMode = writeConf.distributionMode();
      return SparkDistributionAndOrderingUtil.buildRequiredDistribution(table, distributionMode);
    }
  }

  private DistributionMode copyOnWriteDistributionMode() {
    switch (copyOnWriteCommand) {
      case DELETE:
        return writeConf.deleteDistributionMode();
      case UPDATE:
        return writeConf.updateDistributionMode();
      case MERGE:
        return writeConf.copyOnWriteMergeDistributionMode();
      default:
        throw new IllegalArgumentException("Unexpected command: " + copyOnWriteCommand);
    }
  }

  private SortOrder[] buildRequiredOrdering(Distribution requiredDistribution) {
    if (overwriteFiles) {
      return SparkDistributionAndOrderingUtil.buildCopyOnWriteOrdering(
          table, copyOnWriteCommand, requiredDistribution);
    } else {
      return SparkDistributionAndOrderingUtil.buildRequiredOrdering(table, requiredDistribution);
    }
  }

  private boolean allIdentityTransforms(PartitionSpec spec) {
    return spec.fields().stream().allMatch(field -> field.transform().isIdentity());
  }

  private static Schema validateOrMergeWriteSchema(
      Table table, StructType dsSchema, SparkWriteConf writeConf) {
    Schema writeSchema;
    boolean caseSensitive = writeConf.caseSensitive();
    if (writeConf.mergeSchema()) {
      // convert the dataset schema and assign fresh ids for new fields
      Schema newSchema =
          SparkSchemaUtil.convertWithFreshIds(table.schema(), dsSchema, caseSensitive);

      // update the table to get final id assignments and validate the changes
      UpdateSchema update =
          table.updateSchema().caseSensitive(caseSensitive).unionByNameWith(newSchema);
      Schema mergedSchema = update.apply();

      // reconvert the dsSchema without assignment to use the ids assigned by UpdateSchema
      writeSchema = SparkSchemaUtil.convert(mergedSchema, dsSchema, caseSensitive);

      TypeUtil.validateWriteSchema(
          mergedSchema, writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());

      // if the validation passed, update the table schema
      update.commit();
    } else {
      writeSchema = SparkSchemaUtil.convert(table.schema(), dsSchema, caseSensitive);
      TypeUtil.validateWriteSchema(
          table.schema(), writeSchema, writeConf.checkNullability(), writeConf.checkOrdering());
    }

    return writeSchema;
  }
}
