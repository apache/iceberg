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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.source.metrics.NumDeletes;
import org.apache.iceberg.spark.source.metrics.NumSplits;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkScan implements Scan, SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(SparkScan.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final boolean caseSensitive;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final boolean readTimestampWithoutZone;

  // lazy variables
  private StructType readSchema;

  SparkScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {

    SparkSchemaUtil.validateMetadataColumnReferences(table.schema(), expectedSchema);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.readConf = readConf;
    this.caseSensitive = readConf.caseSensitive();
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filters != null ? filters : Collections.emptyList();
    this.readTimestampWithoutZone = readConf.handleTimestampWithoutZone();
  }

  protected Table table() {
    return table;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected Schema expectedSchema() {
    return expectedSchema;
  }

  protected List<Expression> filterExpressions() {
    return filterExpressions;
  }

  protected abstract List<? extends ScanTaskGroup<?>> taskGroups();

  @Override
  public Batch toBatch() {
    return new SparkBatch(sparkContext, table, readConf, taskGroups(), expectedSchema, hashCode());
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new SparkMicroBatchStream(
        sparkContext, table, readConf, expectedSchema, checkpointLocation);
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      Preconditions.checkArgument(
          readTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(expectedSchema),
          SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);
      this.readSchema = SparkSchemaUtil.convert(expectedSchema);
    }
    return readSchema;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(table.currentSnapshot());
  }

  protected Statistics estimateStatistics(Snapshot snapshot) {
    // its a fresh table, no data
    if (snapshot == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables (metadata tables are
    // unpartitioned)
    if (!table.spec().isUnpartitioned() && filterExpressions.isEmpty()) {
      LOG.debug("using table metadata to estimate table statistics");
      long totalRecords =
          PropertyUtil.propertyAsLong(
              snapshot.summary(), SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
      return new Stats(SparkSchemaUtil.estimateSize(readSchema(), totalRecords), totalRecords);
    }

    long rowsCount = taskGroups().stream().mapToLong(ScanTaskGroup::estimatedRowsCount).sum();
    long sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
    return new Stats(sizeInBytes, rowsCount);
  }

  @Override
  public String description() {
    String filters =
        filterExpressions.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table, filters);
  }

  @Override
  public CustomMetric[] supportedCustomMetrics() {
    return new CustomMetric[] {new NumSplits(), new NumDeletes()};
  }
}
