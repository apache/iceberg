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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.spark.SparkWriteRequirements;
import org.apache.iceberg.spark.source.SparkWriteBuilder.Mode.Append;
import org.apache.iceberg.spark.source.SparkWriteBuilder.Mode.CopyOnWriteOperation;
import org.apache.iceberg.spark.source.SparkWriteBuilder.Mode.DynamicOverwrite;
import org.apache.iceberg.spark.source.SparkWriteBuilder.Mode.OverwriteByFilter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.SparkSession;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class SparkWriteBuilder implements WriteBuilder, SupportsDynamicOverwrite, SupportsOverwrite {
  private final SparkSession spark;
  private final Table table;
  private final String branch;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo info;
  private final boolean caseSensitive;
  private final boolean checkNullability;
  private final boolean checkOrdering;
  private final boolean mergeSchema;
  private Mode mode = null;

  SparkWriteBuilder(SparkSession spark, Table table, String branch, LogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.branch = branch;
    this.writeConf = new SparkWriteConf(spark, table, info.options());
    this.info = info;
    this.caseSensitive = writeConf.caseSensitive();
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
    this.mergeSchema = writeConf.mergeSchema();
    SparkTableUtil.validateWriteBranch(spark, table, branch, info.options());
  }

  public WriteBuilder overwriteFiles(Scan scan, Command command, IsolationLevel isolationLevel) {
    Preconditions.checkState(mode == null, "Cannot use copy-on-write with other modes");
    this.mode = new CopyOnWriteOperation((SparkCopyOnWriteScan) scan, command, isolationLevel);
    return this;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    Preconditions.checkState(mode == null, "Cannot use dynamic overwrite with other modes");
    this.mode = new DynamicOverwrite();
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    Preconditions.checkState(mode == null, "Cannot use overwrite by filter with other modes");
    Expression expr = SparkFilters.convert(filters);
    this.mode = useDynamicOverwrite(expr) ? new DynamicOverwrite() : new OverwriteByFilter(expr);
    return this;
  }

  private boolean useDynamicOverwrite(Expression expr) {
    return expr == Expressions.alwaysTrue() && "dynamic".equals(writeConf.overwriteMode());
  }

  private boolean writeNeedsRowLineage() {
    return TableUtil.supportsRowLineage(table) && mode instanceof CopyOnWriteOperation;
  }

  private boolean writeIncludesRowLineage() {
    return info.metadataSchema()
        .map(schema -> schema.exists(field -> field.name().equals(MetadataColumns.ROW_ID.name())))
        .orElse(false);
  }

  private StructType sparkWriteSchema() {
    if (writeIncludesRowLineage()) {
      StructType writeSchema = info.schema();
      StructType metaSchema = info.metadataSchema().get();
      StructField rowId = metaSchema.apply(MetadataColumns.ROW_ID.name());
      writeSchema = writeSchema.add(rowId);
      StructField rowSeq = metaSchema.apply(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
      writeSchema = writeSchema.add(rowSeq);
      return writeSchema;
    } else {
      return info.schema();
    }
  }

  @Override
  public Write build() {
    validateRowLineage();
    Schema writeSchema = mergeSchema ? mergeAndValidateWriteSchema() : validateWriteSchema();
    SparkUtil.validatePartitionTransforms(table.spec());
    String appId = spark.sparkContext().applicationId();

    return new SparkWrite(
        spark,
        table,
        branch,
        writeConf,
        info,
        appId,
        writeSchema,
        sparkWriteSchema(),
        writeRequirements()) {

      @Override
      public BatchWrite toBatch() {
        if (mode instanceof OverwriteByFilter overwrite) {
          return asOverwriteByFilter(overwrite.expr());
        } else if (mode instanceof DynamicOverwrite) {
          return asDynamicOverwrite();
        } else if (mode instanceof CopyOnWriteOperation cow) {
          return asCopyOnWriteOperation(cow.scan(), cow.isolationLevel());
        } else {
          return asBatchAppend();
        }
      }

      @Override
      public StreamingWrite toStreaming() {
        if (mode instanceof OverwriteByFilter overwrite) {
          Preconditions.checkState(
              overwrite.expr() == Expressions.alwaysTrue(),
              "Unsupported streaming overwrite filter: " + overwrite.expr());
          return asStreamingOverwrite();
        } else if (mode == null || mode instanceof Append) {
          return asStreamingAppend();
        } else {
          throw new IllegalStateException("Unsupported streaming write mode: " + mode);
        }
      }
    };
  }

  private SparkWriteRequirements writeRequirements() {
    if (mode instanceof CopyOnWriteOperation cow) {
      return writeConf.copyOnWriteRequirements(cow.command());
    } else {
      return writeConf.writeRequirements();
    }
  }

  private void validateRowLineage() {
    Preconditions.checkArgument(
        writeIncludesRowLineage() || !writeNeedsRowLineage(),
        "Row lineage information is missing for write in mode: %s",
        mode);
  }

  private Schema validateWriteSchema() {
    Schema writeSchema = SparkSchemaUtil.convert(table.schema(), info.schema(), caseSensitive);
    TypeUtil.validateWriteSchema(table.schema(), writeSchema, checkNullability, checkOrdering);
    return addRowLineageIfNeeded(writeSchema);
  }

  // merge schema flow:
  // - convert Spark schema and assign fresh IDs for new fields
  // - update table to get final ID assignments and validate changes
  // - reconvert Spark schema without assignment to use IDs assigned by UpdateSchema
  // - if validation passed, update table schema
  private Schema mergeAndValidateWriteSchema() {
    Schema newSchema =
        SparkSchemaUtil.convertWithFreshIds(table.schema(), info.schema(), caseSensitive);
    UpdateSchema update =
        table.updateSchema().caseSensitive(caseSensitive).unionByNameWith(newSchema);
    Schema mergedSchema = update.apply();
    Schema writeSchema = SparkSchemaUtil.convert(mergedSchema, info.schema(), caseSensitive);
    TypeUtil.validateWriteSchema(mergedSchema, writeSchema, checkNullability, checkOrdering);
    update.commit();
    return addRowLineageIfNeeded(writeSchema);
  }

  private Schema addRowLineageIfNeeded(Schema schema) {
    return writeNeedsRowLineage() ? MetadataColumns.schemaWithRowLineage(schema) : schema;
  }

  sealed interface Mode {
    // add new data
    record Append() implements Mode {}

    // overwrite partitions that receive new data (determined at runtime)
    record DynamicOverwrite() implements Mode {}

    // overwrite data files matching filter expression (a.k.a static overwrite)
    record OverwriteByFilter(Expression expr) implements Mode {}

    // copy-on-write operation (UPDATE/DELETE/MERGE) that completely rewrites affected files
    record CopyOnWriteOperation(
        SparkCopyOnWriteScan scan, Command command, IsolationLevel isolationLevel)
        implements Mode {}
  }
}
