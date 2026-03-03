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

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.TypeUtil.GetID;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base Spark scan builder with common functionality like projection and predicate pushdown.
 *
 * <p>Note that this class intentionally doesn't implement any optional mix-in Spark interfaces even
 * if it contains necessary logic, allowing each concrete scan implementation to select what
 * functionality is applicable to that scan.
 */
abstract class BaseSparkScanBuilder implements ScanBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(BaseSparkScanBuilder.class);
  private static final Predicate[] NO_PREDICATES = new Predicate[0];

  private final SparkSession spark;
  private final Table table;
  private final Schema schema;
  private final SparkReadConf readConf;
  private final boolean caseSensitive;
  private final Set<String> metaFieldNames = Sets.newLinkedHashSet();
  private final InMemoryMetricsReporter metricsReporter = new InMemoryMetricsReporter();

  private Schema projection;
  private List<Expression> filters = Lists.newArrayList();
  private Predicate[] pushedPredicates = NO_PREDICATES;
  private Integer limit = null;

  protected BaseSparkScanBuilder(
      SparkSession spark, Table table, Schema schema, CaseInsensitiveStringMap options) {
    this(spark, table, schema, null, options);
  }

  protected BaseSparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      String branch,
      CaseInsensitiveStringMap options) {
    this.spark = spark;
    this.table = table;
    this.schema = schema;
    this.readConf = new SparkReadConf(spark, table, branch, options);
    this.caseSensitive = readConf.caseSensitive();
    this.projection = schema;
  }

  protected SparkSession spark() {
    return spark;
  }

  protected Table table() {
    return table;
  }

  protected Schema schema() {
    return schema;
  }

  protected Schema projection() {
    return projection;
  }

  protected SparkReadConf readConf() {
    return readConf;
  }

  protected boolean caseSensitive() {
    return caseSensitive;
  }

  protected List<Expression> filters() {
    return filters;
  }

  protected Expression filter() {
    return filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
  }

  protected InMemoryMetricsReporter metricsReporter() {
    return metricsReporter;
  }

  // logic necessary for SupportsPushDownRequiredColumns
  public void pruneColumns(StructType requestedType) {
    List<StructField> dataFields = Lists.newArrayList();

    for (StructField field : requestedType.fields()) {
      if (MetadataColumns.isMetadataColumn(field.name())) {
        metaFieldNames.add(field.name());
      } else {
        dataFields.add(field);
      }
    }

    StructType requestedDataType = SparkSchemaUtil.toStructType(dataFields);
    this.projection = SparkSchemaUtil.prune(projection, requestedDataType, filter(), caseSensitive);
  }

  // logic necessary for SupportsPushDownV2Filters
  public Predicate[] pushPredicates(Predicate[] predicates) {
    // there are 3 kinds of filters:
    // (1) filters that can be pushed down completely and don't have to evaluated by Spark
    //     (e.g. filters that select entire partitions)
    // (2) filters that can be pushed down partially and require record-level filtering in Spark
    //     (e.g. filters that may select some but not necessarily all rows in a file)
    // (3) filters that can't be pushed down at all and have to be evaluated by Spark
    //     (e.g. unsupported filters)
    // filters (1) and (2) are used to prune files during job planning in Iceberg
    // filters (2) and (3) form a set of post scan filters and must be evaluated by Spark

    List<Expression> expressions = Lists.newArrayListWithExpectedSize(predicates.length);
    List<Predicate> pushablePredicates = Lists.newArrayListWithExpectedSize(predicates.length);
    List<Predicate> postScanPredicates = Lists.newArrayListWithExpectedSize(predicates.length);

    for (Predicate predicate : predicates) {
      try {
        Expression expr = SparkV2Filters.convert(predicate);

        if (expr != null) {
          // try binding the expression to ensure it can be pushed down
          Binder.bind(projection.asStruct(), expr, caseSensitive);
          expressions.add(expr);
          pushablePredicates.add(predicate);
        }

        if (expr == null || !ExpressionUtil.selectsPartitions(expr, table, caseSensitive)) {
          postScanPredicates.add(predicate);
        } else {
          LOG.info("Evaluating completely on Iceberg side: {}", predicate);
        }

      } catch (Exception e) {
        LOG.warn("Failed to check if {} can be pushed down: {}", predicate, e.getMessage());
        postScanPredicates.add(predicate);
      }
    }

    this.filters = expressions;
    this.pushedPredicates = pushablePredicates.toArray(new Predicate[0]);

    return postScanPredicates.toArray(new Predicate[0]);
  }

  // logic necessary for SupportsPushDownV2Filters
  public Predicate[] pushedPredicates() {
    return pushedPredicates;
  }

  // logic necessary for SupportsPushDownLimit
  public boolean pushLimit(int newLimit) {
    this.limit = newLimit;
    return true;
  }

  // schema of rows that must be returned by readers
  protected Schema projectionWithMetadataColumns() {
    return TypeUtil.join(projection, calculateMetadataSchema());
  }

  // computes metadata schema avoiding conflicts between partition and data field IDs
  private Schema calculateMetadataSchema() {
    List<Types.NestedField> metaFields = metaFields();
    Optional<Types.NestedField> partitionField = findPartitionField(metaFields);

    if (partitionField.isEmpty()) {
      return new Schema(metaFields);
    }

    Types.StructType partitionType = partitionField.get().type().asStructType();
    Set<Integer> partitionFieldIds = TypeUtil.getProjectedIds(partitionType);
    GetID getId = TypeUtil.reassignConflictingIds(partitionFieldIds, allUsedFieldIds());
    return new Schema(metaFields, getId);
  }

  private List<Types.NestedField> metaFields() {
    return metaFieldNames.stream()
        .map(name -> MetadataColumns.metadataColumn(table, name))
        .collect(Collectors.toList());
  }

  private Optional<Types.NestedField> findPartitionField(List<Types.NestedField> fields) {
    return fields.stream()
        .filter(field -> MetadataColumns.PARTITION_COLUMN_ID == field.fieldId())
        .findFirst();
  }

  // collects used data field IDs across all known table schemas
  private Set<Integer> allUsedFieldIds() {
    return table.schemas().values().stream()
        .flatMap(tableSchema -> TypeUtil.getProjectedIds(tableSchema.asStruct()).stream())
        .collect(Collectors.toSet());
  }

  protected <T extends Scan<T, ?, ?>> T configureSplitPlanning(T scan) {
    T newScan = scan;

    Long splitSize = readConf.splitSizeOption();
    if (splitSize != null) {
      newScan = newScan.option(SPLIT_SIZE, String.valueOf(splitSize));
    }

    Integer splitLookback = readConf.splitLookbackOption();
    if (splitLookback != null) {
      newScan = newScan.option(SPLIT_LOOKBACK, String.valueOf(splitLookback));
    }

    Long splitOpenFileCost = readConf.splitOpenFileCostOption();
    if (splitOpenFileCost != null) {
      newScan = newScan.option(SPLIT_OPEN_FILE_COST, String.valueOf(splitOpenFileCost));
    }

    if (limit != null) {
      newScan = newScan.minRowsRequested(limit.longValue());
    }

    return newScan;
  }
}
