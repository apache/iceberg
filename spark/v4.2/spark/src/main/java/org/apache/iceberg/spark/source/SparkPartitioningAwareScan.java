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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.PartitionFieldReference;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate;
import org.apache.spark.sql.connector.read.SupportsReportOrdering;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkPartitioningAwareScan<T extends PartitionScanTask> extends SparkScan
    implements SupportsReportPartitioning, SupportsReportOrdering {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPartitioningAwareScan.class);
  private static final SortOrder[] NO_ORDERING = new SortOrder[0];

  private final Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan;
  private final boolean preserveDataGrouping;
  private final List<PartitionPredicate> partitionPredicates;
  private final List<PartitionField> partitionPredicateFields;
  private final StructType partitionPredicateType;
  private final boolean preserveDataOrdering;
  private final int maxFilesPerOrderedTask;

  private Set<PartitionSpec> specs = null; // lazy cache of scanned specs
  private List<T> tasks = null; // lazy cache of uncombined tasks
  private List<ScanTaskGroup<T>> taskGroups = null; // lazy cache of task groups
  private StructType groupingKeyType = null; // lazy cache of the grouping key type
  private Transform[] groupingKeyTransforms = null; // lazy cache of grouping key transforms
  private SortOrder[] reportedOrdering = null; // lazy cache of the ordering, empty if not reported

  SparkPartitioningAwareScan(
      SparkSession spark,
      Table table,
      Schema schema,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      List<PartitionPredicate> partitionPredicates,
      Supplier<ScanReport> scanReportSupplier) {
    super(
        spark,
        table,
        null != scan ? scan.fileIO() : table::io,
        schema,
        readConf,
        projection,
        filters,
        scanReportSupplier);

    this.scan = scan;
    this.preserveDataGrouping = readConf.preserveDataGrouping();
    this.partitionPredicates = Lists.newArrayList(partitionPredicates);
    this.partitionPredicateFields = activePartitionFields(table.spec());
    this.partitionPredicateType = activePartitionType(table.spec());
    this.preserveDataOrdering =
        readConf.preserveDataOrdering() && canPreserveDataOrdering(spark, table);
    this.maxFilesPerOrderedTask =
        preserveDataOrdering ? readConf.preserveDataOrderingMaxFilesPerTask() : 0;

    if (scan == null) {
      this.specs = Collections.emptySet();
      this.tasks = Collections.emptyList();
      this.taskGroups = Collections.emptyList();
      this.reportedOrdering = NO_ORDERING;
    }
  }

  private boolean canPreserveDataOrdering(SparkSession spark, Table table) {
    SQLConf sqlConf = spark.sessionState().conf();
    if (!preserveDataGrouping) {
      LOG.warn(
          "Ignoring {} for table {}: it requires {}",
          SparkSQLProperties.PRESERVE_DATA_ORDERING,
          table.name(),
          SparkSQLProperties.PRESERVE_DATA_GROUPING);
      return false;

    } else if (sqlConf.v2BucketingAllowCompatibleTransforms()) {
      LOG.warn(
          "Ignoring {} for table {}: incompatible with {}",
          SparkSQLProperties.PRESERVE_DATA_ORDERING,
          table.name(),
          SQLConf.V2_BUCKETING_ALLOW_COMPATIBLE_TRANSFORMS().key());
      return false;

    } else if (sqlConf.v2BucketingAllowJoinKeysSubsetOfPartitionKeys()) {
      LOG.warn(
          "Ignoring {} for table {}: incompatible with {}",
          SparkSQLProperties.PRESERVE_DATA_ORDERING,
          table.name(),
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS().key());
      return false;
    }

    return true;
  }

  protected abstract Class<T> taskJavaClass();

  protected Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan() {
    return scan;
  }

  @Override
  public Partitioning outputPartitioning() {
    if (groupingKeyType().fields().isEmpty()) {
      LOG.info(
          "Reporting UnknownPartitioning with {} partition(s) for table {}",
          taskGroups().size(),
          table().name());
      return new UnknownPartitioning(taskGroups().size());
    } else {
      LOG.info(
          "Reporting KeyGroupedPartitioning by {} with {} partition(s) for table {}",
          groupingKeyTransforms(),
          taskGroups().size(),
          table().name());
      return new KeyGroupedPartitioning(groupingKeyTransforms(), taskGroups().size());
    }
  }

  @Override
  public synchronized SortOrder[] outputOrdering() {
    taskGroups();
    return reportedOrdering;
  }

  @Override
  protected StructType groupingKeyType() {
    if (groupingKeyType == null) {
      if (preserveDataGrouping) {
        this.groupingKeyType = computeGroupingKeyType();
      } else {
        this.groupingKeyType = StructType.of();
      }
    }

    return groupingKeyType;
  }

  private StructType computeGroupingKeyType() {
    return org.apache.iceberg.Partitioning.groupingKeyType(projection(), specs());
  }

  private Transform[] groupingKeyTransforms() {
    if (groupingKeyTransforms == null) {
      Map<Integer, PartitionField> fieldsById = indexFieldsById(specs());

      List<PartitionField> groupingKeyFields =
          groupingKeyType().fields().stream()
              .map(field -> fieldsById.get(field.fieldId()))
              .collect(Collectors.toList());

      this.groupingKeyTransforms = Spark3Util.toTransforms(schema(), groupingKeyFields);
    }

    return groupingKeyTransforms;
  }

  private Map<Integer, PartitionField> indexFieldsById(Iterable<PartitionSpec> specIterable) {
    Map<Integer, PartitionField> fieldsById = Maps.newHashMap();

    for (PartitionSpec spec : specIterable) {
      for (PartitionField field : spec.fields()) {
        fieldsById.putIfAbsent(field.fieldId(), field);
      }
    }

    return fieldsById;
  }

  protected Set<PartitionSpec> specs() {
    if (specs == null) {
      // avoid calling equals/hashCode on specs as those methods are relatively expensive
      IntStream specIds = tasks().stream().mapToInt(task -> task.spec().specId()).distinct();
      this.specs = specIds.mapToObj(id -> table().specs().get(id)).collect(Collectors.toSet());
    }

    return specs;
  }

  protected synchronized List<T> tasks() {
    if (tasks == null) {
      try (CloseableIterable<? extends ScanTask> taskIterable = scan.planFiles()) {
        List<T> plannedTasks = Lists.newArrayList();
        Map<Integer, PartitionPredicateEvaluator> evaluatorsBySpecId = Maps.newHashMap();
        int numCandidateTasks = 0;

        for (ScanTask task : taskIterable) {
          ValidationException.check(
              taskJavaClass().isInstance(task),
              "Unsupported task type, expected a subtype of %s: %s",
              taskJavaClass().getName(),
              task.getClass().getName());

          T partitionTask = taskJavaClass().cast(task);
          numCandidateTasks += 1;

          if (partitionPredicates.isEmpty()
              || matchesPartitionPredicates(partitionTask, evaluatorsBySpecId)) {
            plannedTasks.add(partitionTask);
          }
        }

        this.tasks = plannedTasks;

        if (plannedTasks.size() < numCandidateTasks) {
          LOG.info(
              "{} of {} task(s) for table {} matched {} opaque Spark partition predicate(s)",
              plannedTasks.size(),
              numCandidateTasks,
              table().name(),
              partitionPredicates.size());
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close scan: " + scan, e);
      }
    }

    return tasks;
  }

  @Override
  protected synchronized List<ScanTaskGroup<T>> taskGroups() {
    if (taskGroups == null) {
      long splitSize = adjustSplitSize(tasks(), scan.targetSplitSize());

      if (groupingKeyType().fields().isEmpty()) {
        CloseableIterable<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                CloseableIterable.withNoopClose(tasks()),
                splitSize,
                scan.splitLookback(),
                scan.splitOpenFileCost());
        this.taskGroups = Lists.newArrayList(plannedTaskGroups);
        this.reportedOrdering = NO_ORDERING;

        LOG.debug(
            "Planned {} task group(s) without data grouping for table {}",
            taskGroups.size(),
            table().name());

      } else {
        List<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                tasks(),
                splitSize,
                scan.splitLookback(),
                scan.splitOpenFileCost(),
                groupingKeyType());
        if (reportedOrdering == null) {
          this.reportedOrdering = reportableOrdering(plannedTaskGroups);
        }

        if (reportedOrdering.length > 0) {
          plannedTaskGroups = asOrderedTaskGroups(plannedTaskGroups);
        }

        StructLikeSet plannedGroupingKeys = collectGroupingKeys(plannedTaskGroups);

        LOG.debug(
            "Planned {} task group(s) with {} grouping key type and {} unique grouping key(s) for table {}",
            plannedTaskGroups.size(),
            groupingKeyType(),
            plannedGroupingKeys.size(),
            table().name());

        this.taskGroups = plannedTaskGroups;
      }

      setEffectiveSplitTargetSize(taskGroups, splitSize);
    }

    return taskGroups;
  }

  private SortOrder[] reportableOrdering(List<ScanTaskGroup<T>> plannedTaskGroups) {
    org.apache.iceberg.SortOrder sortOrder = table().sortOrder();
    int prefixLength = sortOrderPrefixLength(sortOrder);
    if (prefixLength > 0
        && SparkOrderingUtil.withinFileLimit(plannedTaskGroups, maxFilesPerOrderedTask)) {
      LOG.info(
          "Reporting {} field(s) of sort order {} for table {}",
          prefixLength,
          sortOrder.orderId(),
          table().name());
      return Arrays.copyOf(Spark3Util.toOrdering(sortOrder, schema()), prefixLength);
    }

    return NO_ORDERING;
  }

  private List<ScanTaskGroup<T>> asOrderedTaskGroups(List<ScanTaskGroup<T>> plannedTaskGroups) {
    return plannedTaskGroups.stream()
        .<ScanTaskGroup<T>>map(OrderedScanTaskGroup::new)
        .collect(Collectors.toList());
  }

  private int sortOrderPrefixLength(org.apache.iceberg.SortOrder sortOrder) {
    if (!preserveDataOrdering) {
      return 0;
    }

    int prefixLength =
        SparkOrderingUtil.reportablePrefixLength(sortOrder, table().schema(), projection());
    boolean mergeable =
        prefixLength > 0 && SparkOrderingUtil.canMerge(tasks(), sortOrder.orderId());
    return mergeable ? prefixLength : 0;
  }

  // only task groups can be reset while resetting tasks
  // the set of scanned specs and grouping key type must never change
  protected synchronized void resetTasks(List<T> filteredTasks) {
    this.taskGroups = null;
    this.tasks = filteredTasks;
    resetEffectiveSplitTargetSize();
  }

  private StructLikeSet collectGroupingKeys(Iterable<ScanTaskGroup<T>> taskGroupIterable) {
    StructLikeSet keys = StructLikeSet.create(groupingKeyType());

    for (ScanTaskGroup<T> taskGroup : taskGroupIterable) {
      keys.add(taskGroup.groupingKey());
    }

    return keys;
  }

  protected String groupingKeyDesc() {
    return groupingKeyType().fields().stream()
        .map(NestedField::name)
        .collect(Collectors.joining(", "));
  }

  protected List<PartitionPredicate> partitionPredicates() {
    return partitionPredicates;
  }

  private boolean matchesPartitionPredicates(
      T task, Map<Integer, PartitionPredicateEvaluator> evaluatorsBySpecId) {
    PartitionPredicateEvaluator evaluator =
        evaluatorsBySpecId.computeIfAbsent(
            task.spec().specId(), ignored -> new PartitionPredicateEvaluator(task.spec()));
    return evaluator.eval(task.partition());
  }

  /**
   * Returns the planned tasks that match all partition predicates in the scan.
   *
   * <p>Tasks from specs where a predicate cannot be evaluated are kept.
   */
  protected synchronized List<T> tasksMatchingPartitionPredicates() {
    Map<Integer, PartitionPredicateEvaluator> evaluatorsBySpecId = Maps.newHashMap();
    return tasks().stream()
        .filter(task -> matchesPartitionPredicates(task, evaluatorsBySpecId))
        .collect(Collectors.toList());
  }

  private static List<PartitionField> activePartitionFields(PartitionSpec spec) {
    return spec.fields().stream()
        .filter(field -> !field.transform().isVoid())
        .collect(Collectors.toList());
  }

  private static StructType activePartitionType(PartitionSpec spec) {
    List<NestedField> activeFields = Lists.newArrayList();
    List<NestedField> partitionTypeFields = spec.partitionType().fields();

    for (int pos = 0; pos < spec.fields().size(); pos += 1) {
      if (!spec.fields().get(pos).transform().isVoid()) {
        activeFields.add(partitionTypeFields.get(pos));
      }
    }

    return StructType.of(activeFields);
  }

  private class PartitionPredicateEvaluator {
    private final StructProjection partitionProjection;
    private final StructInternalRow partitionKey;
    private final boolean[] evaluablePredicates;

    private PartitionPredicateEvaluator(PartitionSpec spec) {
      this.partitionProjection =
          StructProjection.createAllowMissing(spec.partitionType(), partitionPredicateType);
      this.partitionKey = new StructInternalRow(partitionPredicateType);
      this.evaluablePredicates = findEvaluablePredicates(spec);
    }

    private boolean eval(StructLike partition) {
      partitionKey.setStruct(partitionProjection.wrap(partition));

      for (int pos = 0; pos < partitionPredicates.size(); pos += 1) {
        if (evaluablePredicates[pos] && !partitionPredicates.get(pos).eval(partitionKey)) {
          return false;
        }
      }

      return true;
    }

    private boolean[] findEvaluablePredicates(PartitionSpec spec) {
      Map<Integer, PartitionField> fieldsById = Maps.newHashMap();
      for (PartitionField field : spec.fields()) {
        if (!field.transform().isVoid()) {
          fieldsById.put(field.fieldId(), field);
        }
      }

      boolean[] result = new boolean[partitionPredicates.size()];
      for (int pos = 0; pos < partitionPredicates.size(); pos += 1) {
        result[pos] = canEvaluatePredicate(partitionPredicates.get(pos), fieldsById);
      }

      return result;
    }

    private boolean canEvaluatePredicate(
        PartitionPredicate predicate, Map<Integer, PartitionField> fieldsById) {
      for (NamedReference reference : predicate.references()) {
        if (!(reference instanceof PartitionFieldReference partitionReference)) {
          return false;
        }

        int ordinal = partitionReference.ordinal();
        if (ordinal < 0 || ordinal >= partitionPredicateFields.size()) {
          return false;
        }

        PartitionField currentField = partitionPredicateFields.get(ordinal);
        PartitionField specField = fieldsById.get(currentField.fieldId());
        if (specField == null || !specField.transform().equals(currentField.transform())) {
          return false;
        }
      }

      return true;
    }
  }

  protected String orderingDesc() {
    return Arrays.stream(outputOrdering())
        .map(SortOrder::describe)
        .collect(Collectors.joining(", "));
  }
}
