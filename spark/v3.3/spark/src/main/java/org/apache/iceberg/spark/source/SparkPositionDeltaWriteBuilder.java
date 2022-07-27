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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkDistributionAndOrderingUtil;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.iceberg.write.DeltaWrite;
import org.apache.spark.sql.connector.iceberg.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.iceberg.write.ExtendedLogicalWriteInfo;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.types.StructType;

class SparkPositionDeltaWriteBuilder implements DeltaWriteBuilder {

  private static final Schema EXPECTED_ROW_ID_SCHEMA =
      new Schema(MetadataColumns.FILE_PATH, MetadataColumns.ROW_POSITION);

  private final SparkSession spark;
  private final Table table;
  private final Command command;
  private final SparkBatchQueryScan scan;
  private final IsolationLevel isolationLevel;
  private final SparkWriteConf writeConf;
  private final ExtendedLogicalWriteInfo info;
  private final boolean handleTimestampWithoutZone;
  private final boolean checkNullability;
  private final boolean checkOrdering;

  SparkPositionDeltaWriteBuilder(
      SparkSession spark,
      Table table,
      Command command,
      Scan scan,
      IsolationLevel isolationLevel,
      ExtendedLogicalWriteInfo info) {
    this.spark = spark;
    this.table = table;
    this.command = command;
    this.scan = (SparkBatchQueryScan) scan;
    this.isolationLevel = isolationLevel;
    this.writeConf = new SparkWriteConf(spark, table, info.options());
    this.info = info;
    this.handleTimestampWithoutZone = writeConf.handleTimestampWithoutZone();
    this.checkNullability = writeConf.checkNullability();
    this.checkOrdering = writeConf.checkOrdering();
  }

  @Override
  public DeltaWrite build() {
    Preconditions.checkArgument(
        handleTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(table.schema()),
        SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);

    Schema dataSchema = dataSchema();
    if (dataSchema != null) {
      TypeUtil.validateWriteSchema(table.schema(), dataSchema, checkNullability, checkOrdering);
    }

    Schema rowIdSchema = SparkSchemaUtil.convert(EXPECTED_ROW_ID_SCHEMA, info.rowIdSchema());
    TypeUtil.validateSchema(
        "row ID", EXPECTED_ROW_ID_SCHEMA, rowIdSchema, checkNullability, checkOrdering);

    NestedField partition =
        MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME);
    Schema expectedMetadataSchema = new Schema(MetadataColumns.SPEC_ID, partition);
    Schema metadataSchema = SparkSchemaUtil.convert(expectedMetadataSchema, info.metadataSchema());
    TypeUtil.validateSchema(
        "metadata", expectedMetadataSchema, metadataSchema, checkNullability, checkOrdering);

    SparkUtil.validatePartitionTransforms(table.spec());

    Distribution distribution =
        SparkDistributionAndOrderingUtil.buildPositionDeltaDistribution(
            table, command, distributionMode());
    SortOrder[] ordering =
        SparkDistributionAndOrderingUtil.buildPositionDeltaOrdering(table, command);

    return new SparkPositionDeltaWrite(
        spark,
        table,
        command,
        scan,
        isolationLevel,
        writeConf,
        info,
        dataSchema,
        distribution,
        ordering);
  }

  private Schema dataSchema() {
    StructType dataSparkType = info.schema();
    return dataSparkType != null ? SparkSchemaUtil.convert(table.schema(), dataSparkType) : null;
  }

  private DistributionMode distributionMode() {
    switch (command) {
      case DELETE:
        return writeConf.deleteDistributionMode();
      case UPDATE:
        return writeConf.updateDistributionMode();
      case MERGE:
        return writeConf.positionDeltaMergeDistributionMode();
      default:
        throw new IllegalArgumentException("Unexpected command: " + command);
    }
  }
}
