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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkFilters;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkBatchQueryScan extends SparkBatchScan implements SupportsRuntimeFiltering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchQueryScan.class);

  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final List<Expression> runtimeFilterExpressions;

  private TableScan tableScan = null; // lazy scan
  private Set<Integer> specIds = null; // lazy cache of scanned spec IDs
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks

  SparkBatchQueryScan(SparkSession spark, Table table, SparkReadConf readConf,
                      Schema expectedSchema, List<Expression> filters) {

    super(spark, table, readConf, expectedSchema, filters);

    this.snapshotId = readConf.snapshotId();
    this.asOfTimestamp = readConf.asOfTimestamp();

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either " +
                SparkReadOptions.SNAPSHOT_ID + " or " + SparkReadOptions.AS_OF_TIMESTAMP + " is specified");
      }
    } else if (startSnapshotId == null && endSnapshotId != null) {
      throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
    }

    this.splitSize = readConf.splitSizeOption();
    this.splitLookback = readConf.splitLookbackOption();
    this.splitOpenFileCost = readConf.splitOpenFileCostOption();
    this.runtimeFilterExpressions = Lists.newArrayList();
  }

  private TableScan scan() {
    if (tableScan == null) {
      this.tableScan = buildScan();
    }

    return tableScan;
  }

  private TableScan buildScan() {
    TableScan scan = table()
        .newScan()
        .caseSensitive(caseSensitive())
        .project(expectedSchema());

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    if (asOfTimestamp != null) {
      scan = scan.asOfTime(asOfTimestamp);
    }

    if (startSnapshotId != null) {
      if (endSnapshotId != null) {
        scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
      } else {
        scan = scan.appendsAfter(startSnapshotId);
      }
    }

    if (splitSize != null) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }

    if (splitLookback != null) {
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, String.valueOf(splitLookback));
    }

    if (splitOpenFileCost != null) {
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(splitOpenFileCost));
    }

    for (Expression filter : filterExpressions()) {
      scan = scan.filter(filter);
    }

    return scan;
  }

  private Set<Integer> specIds() {
    if (specIds == null) {
      Set<Integer> specIdSet = Sets.newHashSet();
      for (FileScanTask file : files()) {
        specIdSet.add(file.spec().specId());
      }
      this.specIds = specIdSet;
    }

    return specIds;
  }

  private List<FileScanTask> files() {
    if (files == null) {
      try (CloseableIterable<FileScanTask> filesIterable = scan().planFiles()) {
        this.files = Lists.newArrayList(filesIterable);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close table scan: " + scan(), e);
      }
    }

    return files;
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files()),
          scan().targetSplitSize());
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, scan().targetSplitSize(),
          scan().splitLookback(), scan().splitOpenFileCost());
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public NamedReference[] filterAttributes() {
    Set<Integer> partitionFieldSourceIds = Sets.newHashSet();

    for (Integer specId : specIds()) {
      PartitionSpec spec = table().specs().get(specId);
      for (PartitionField field : spec.fields()) {
        partitionFieldSourceIds.add(field.sourceId());
      }
    }

    Map<Integer, String> quotedNameById = SparkSchemaUtil.indexQuotedNameById(expectedSchema());

    // the optimizer will look for an equality condition with filter attributes in a join
    // as the scan has been already planned, filtering can only be done on projected attributes
    // that's why only partition source fields that are part of the read schema can be reported

    return partitionFieldSourceIds.stream()
        .filter(fieldId -> expectedSchema().findField(fieldId) != null)
        .map(fieldId -> Spark3Util.toNamedReference(quotedNameById.get(fieldId)))
        .toArray(NamedReference[]::new);
  }

  @Override
  public void filter(Filter[] filters) {
    Expression runtimeFilterExpr = convertRuntimeFilters(filters);

    if (runtimeFilterExpr != Expressions.alwaysTrue()) {
      Map<Integer, Evaluator> evaluatorsBySpecId = Maps.newHashMap();

      for (Integer specId : specIds()) {
        PartitionSpec spec = table().specs().get(specId);
        Expression inclusiveExpr = Projections.inclusive(spec).project(runtimeFilterExpr);
        Evaluator inclusive = new Evaluator(spec.partitionType(), inclusiveExpr);
        evaluatorsBySpecId.put(specId, inclusive);
      }

      LOG.info("Trying to filter {} files using runtime filter {}", files().size(), runtimeFilterExpr);

      List<FileScanTask> filteredFiles = files().stream()
          .filter(file -> {
            Evaluator evaluator = evaluatorsBySpecId.get(file.spec().specId());
            return evaluator.eval(file.file().partition());
          })
          .collect(Collectors.toList());

      LOG.info("{}/{} files matched runtime filter {}", filteredFiles.size(), files().size(), runtimeFilterExpr);

      // don't invalidate tasks if the runtime filter had no effect to avoid planning splits again
      if (filteredFiles.size() < files().size()) {
        this.specIds = null;
        this.files = filteredFiles;
        this.tasks = null;
      }

      // save the evaluated filter for equals/hashCode
      runtimeFilterExpressions.add(runtimeFilterExpr);
    }
  }

  // at this moment, Spark can only pass IN filters for a single attribute
  // if there are multiple filter attributes, Spark will pass two separate IN filters
  private Expression convertRuntimeFilters(Filter[] filters) {
    Expression runtimeFilterExpr = Expressions.alwaysTrue();

    for (Filter filter : filters) {
      Expression expr = SparkFilters.convert(filter);
      if (expr != null) {
        try {
          Binder.bind(expectedSchema().asStruct(), expr, caseSensitive());
          runtimeFilterExpr = Expressions.and(runtimeFilterExpr, expr);
        } catch (ValidationException e) {
          LOG.warn("Failed to bind {} to expected schema, skipping runtime filter", expr, e);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", filter);
      }
    }

    return runtimeFilterExpr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString()) &&
        runtimeFilterExpressions.toString().equals(that.runtimeFilterExpressions.toString()) &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), readSchema(), filterExpressions().toString(), runtimeFilterExpressions.toString(),
        snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, runtimeFilters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), runtimeFilterExpressions, caseSensitive());
  }
}
