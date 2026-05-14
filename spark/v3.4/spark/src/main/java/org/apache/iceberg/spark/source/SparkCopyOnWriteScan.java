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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkCopyOnWriteScan extends SparkPartitioningAwareScan<FileScanTask>
    implements SupportsRuntimeV2Filtering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkCopyOnWriteScan.class);

  private final Snapshot snapshot;
  private Set<String> filteredLocations = null;
  private StructType readSchema;

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    this(spark, table, null, null, readConf, expectedSchema, filters, scanReportSupplier);
  }

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      BatchScan scan,
      Snapshot snapshot,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, scan, readConf, expectedSchema, filters, scanReportSupplier);

    this.snapshot = snapshot;

    if (scan == null) {
      this.filteredLocations = Collections.emptySet();
    }
  }

  @Override
  public StructType readSchema() {
    if (readSchema == null) {
      this.readSchema = rowLineageAsDataCols(SparkSchemaUtil.convert(expectedSchema()));
    }

    return readSchema;
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
    NamedReference file = Expressions.column(MetadataColumns.FILE_PATH.name());
    return new NamedReference[] {file};
  }

  @Override
  public void filter(Predicate[] predicates) {
    Preconditions.checkState(
        Objects.equals(snapshotId(), currentSnapshotId()),
        "Runtime file filtering is not possible: the table has been concurrently modified. "
            + "Row-level operation scan snapshot ID: %s, current table snapshot ID: %s. "
            + "If an external process modifies the table, enable table caching in the catalog. "
            + "If multiple threads modify the table, use independent Spark sessions in each thread.",
        snapshotId(),
        currentSnapshotId());

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
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filterExpressions().toString().equals(that.filterExpressions().toString())
        && Objects.equals(snapshotId(), that.snapshotId())
        && Objects.equals(filteredLocations, that.filteredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        readSchema(),
        filterExpressions().toString(),
        snapshotId(),
        filteredLocations);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergCopyOnWriteScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }

  private Long currentSnapshotId() {
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table(), branch());
    return currentSnapshot != null ? currentSnapshot.snapshotId() : null;
  }

  // Indicate to Spark to treat the row id and sequence number as data columns since some optimizer
  // rules for DELETE will not output the row lineage columns otherwise
  private StructType rowLineageAsDataCols(StructType schema) {
    StructField[] fields = new StructField[schema.fields().length];
    for (int i = 0; i < schema.fields().length; i++) {
      StructField field = schema.fields()[i];
      if (isRowLineageField(field)) {
        Metadata updatedMetadata =
            new MetadataBuilder().withMetadata(field.metadata()).remove("__metadata_col").build();
        fields[i] = field.copy(field.name(), field.dataType(), field.nullable(), updatedMetadata);
      } else {
        fields[i] = field;
      }
    }

    return new StructType(fields);
  }

  private boolean isRowLineageField(StructField field) {
    boolean hasLineageFieldName =
        field.name().equals(MetadataColumns.ROW_ID.name())
            || field.name().equals(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
    return hasLineageFieldName && field.metadata().contains("__metadata_col");
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
