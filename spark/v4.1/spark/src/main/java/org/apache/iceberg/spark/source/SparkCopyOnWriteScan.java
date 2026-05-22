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
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkCopyOnWriteScan extends SparkPartitioningAwareScan<FileScanTask>
    implements SupportsRuntimeV2Filtering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkCopyOnWriteScan.class);

  private final Snapshot snapshot;
  private final String branch;

  private Set<String> filteredLocations = null;

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      BatchScan scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, schema, scan, readConf, projection, filters, scanReportSupplier);
    this.snapshot = snapshot;
    this.branch = branch;
    if (scan == null) {
      this.filteredLocations = Collections.emptySet();
    }
  }

  Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  @Override
  protected Class<FileScanTask> taskJavaClass() {
    return FileScanTask.class;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(snapshot);
  }

  @Override
  public NamedReference[] filterAttributes() {
    return new NamedReference[] {SparkMetadataColumns.FILE_PATH.asRef()};
  }

  @Override
  public void filter(Predicate[] predicates) {
    for (Predicate predicate : predicates) {
      // Spark can only pass IN predicates at the moment
      if (isFilePathInPredicate(predicate)) {
        Set<String> fileLocations = extractStringLiterals(predicate);

        // Spark may call this multiple times for UPDATEs with subqueries
        // as such cases are rewritten using UNION and the same scan on both sides
        // so filter files only if it is beneficial
        if (filteredLocations == null || fileLocations.size() < filteredLocations.size()) {
          this.filteredLocations = fileLocations;
          List<FileScanTask> filteredTasks =
              tasks().stream()
                  .filter(file -> fileLocations.contains(file.file().location()))
                  .collect(Collectors.toList());

          LOG.info(
              "{} of {} task(s) for table {} matched runtime file filter with {} location(s)",
              filteredTasks.size(),
              tasks().size(),
              table().name(),
              fileLocations.size());

          resetTasks(filteredTasks);
        }
      } else {
        LOG.warn("Unsupported runtime filter {}", predicate);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkCopyOnWriteScan that = (SparkCopyOnWriteScan) o;
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && Objects.equals(snapshot, that.snapshot)
        && Objects.equals(branch, that.branch)
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filtersDesc().equals(that.filtersDesc())
        && Objects.equals(filteredLocations, that.filteredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        table().uuid(),
        snapshot,
        branch,
        readSchema(),
        filtersDesc(),
        filteredLocations);
  }

  @Override
  public String description() {
    return String.format(
        "IcebergCopyOnWriteScan(table=%s, schemaId=%s, snapshotId=%s, branch=%s, filters=%s, groupedBy=%s)",
        table(), schema().schemaId(), snapshotId(), branch, filtersDesc(), groupingKeyDesc());
  }

  private static boolean isFilePathInPredicate(Predicate predicate) {
    if (!"IN".equals(predicate.name()) || predicate.children().length < 1) {
      return false;
    }

    if (!(predicate.children()[0] instanceof NamedReference)) {
      return false;
    }

    String[] fieldNames = ((NamedReference) predicate.children()[0]).fieldNames();

    return fieldNames.length == 1
        && fieldNames[0].equalsIgnoreCase(MetadataColumns.FILE_PATH.name());
  }

  private static Set<String> extractStringLiterals(Predicate predicate) {
    Set<String> values = Sets.newHashSet();
    for (int i = 1; i < predicate.children().length; i++) {
      if (predicate.children()[i] instanceof Literal) {
        Object value = ((Literal<?>) predicate.children()[i]).value();
        // V2 string literals come through as UTF8String; toString() materializes the Java String
        values.add(value.toString());
      }
    }

    return values;
  }
}
