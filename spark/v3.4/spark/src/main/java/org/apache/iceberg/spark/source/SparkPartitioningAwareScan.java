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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.BaseScanTaskGroup;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.SupportsReportPartitioning;
import org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning;
import org.apache.spark.sql.connector.read.partitioning.Partitioning;
import org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class SparkPartitioningAwareScan<T extends PartitionScanTask> extends SparkScan
    implements SupportsReportPartitioning {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPartitioningAwareScan.class);

  private final Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan;
  private final boolean preserveDataGrouping;

  private Set<PartitionSpec> specs = null; // lazy cache of scanned specs
  private List<T> tasks = null; // lazy cache of uncombined tasks
  private List<ScanTaskGroup<T>> taskGroups = null; // lazy cache of task groups
  private StructType groupingKeyType = null; // lazy cache of the grouping key type
  private Transform[] groupingKeyTransforms = null; // lazy cache of grouping key transforms
  private StructLikeSet groupingKeys = null; // lazy cache of grouping keys

  SparkPartitioningAwareScan(
      SparkSession spark,
      Table table,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {

    super(spark, table, readConf, expectedSchema, filters);

    this.scan = scan;
    this.preserveDataGrouping = readConf.preserveDataGrouping();

    if (scan == null) {
      this.specs = Collections.emptySet();
      this.tasks = Collections.emptyList();
      this.taskGroups = Collections.emptyList();
    }
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
    return org.apache.iceberg.Partitioning.groupingKeyType(expectedSchema(), specs());
  }

  private Transform[] groupingKeyTransforms() {
    if (groupingKeyTransforms == null) {
      Map<Integer, PartitionField> fieldsById = indexFieldsById(specs());

      List<PartitionField> groupingKeyFields =
          groupingKeyType().fields().stream()
              .map(field -> fieldsById.get(field.fieldId()))
              .collect(Collectors.toList());

      Schema schema = SnapshotUtil.schemaFor(table(), branch());
      this.groupingKeyTransforms = Spark3Util.toTransforms(schema, groupingKeyFields);
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

        for (ScanTask task : taskIterable) {
          ValidationException.check(
              taskJavaClass().isInstance(task),
              "Unsupported task type, expected a subtype of %s: %",
              taskJavaClass().getName(),
              task.getClass().getName());

          plannedTasks.add(taskJavaClass().cast(task));
        }

        this.tasks = plannedTasks;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close scan: " + scan, e);
      }
    }

    return tasks;
  }

  @Override
  protected synchronized List<ScanTaskGroup<T>> taskGroups() {
    if (taskGroups == null) {
      if (groupingKeyType().fields().isEmpty()) {
        CloseableIterable<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                CloseableIterable.withNoopClose(tasks()),
                scan.targetSplitSize(),
                scan.splitLookback(),
                scan.splitOpenFileCost());
        this.taskGroups = Lists.newArrayList(plannedTaskGroups);

        LOG.debug(
            "Planned {} task group(s) without data grouping for table {}",
            taskGroups.size(),
            table().name());

      } else {
        List<ScanTaskGroup<T>> plannedTaskGroups =
            TableScanUtil.planTaskGroups(
                tasks(),
                scan.targetSplitSize(),
                scan.splitLookback(),
                scan.splitOpenFileCost(),
                groupingKeyType());
        StructLikeSet plannedGroupingKeys = collectGroupingKeys(plannedTaskGroups);

        LOG.debug(
            "Planned {} task group(s) with {} grouping key type and {} unique grouping key(s) for table {}",
            plannedTaskGroups.size(),
            groupingKeyType(),
            plannedGroupingKeys.size(),
            table().name());

        // task groups may be planned multiple times because of runtime filtering
        // the number of task groups may change but the set of grouping keys must stay same
        // if grouping keys are not null, this planning happens after runtime filtering
        // so an empty task group must be added for each filtered out grouping key

        if (groupingKeys == null) {
          this.taskGroups = plannedTaskGroups;
          this.groupingKeys = plannedGroupingKeys;

        } else {
          StructLikeSet missingGroupingKeys = StructLikeSet.create(groupingKeyType());

          for (StructLike groupingKey : groupingKeys) {
            if (!plannedGroupingKeys.contains(groupingKey)) {
              missingGroupingKeys.add(groupingKey);
            }
          }

          LOG.debug(
              "{} grouping key(s) were filtered out at runtime for table {}",
              missingGroupingKeys.size(),
              table().name());

          for (StructLike groupingKey : missingGroupingKeys) {
            plannedTaskGroups.add(new BaseScanTaskGroup<>(groupingKey, Collections.emptyList()));
          }

          this.taskGroups = plannedTaskGroups;
        }
      }
    }

    return taskGroups;
  }

  // only task groups can be reset while resetting tasks
  // the set of scanned specs, grouping key type, grouping keys must never change
  protected void resetTasks(List<T> filteredTasks) {
    this.taskGroups = null;
    this.tasks = filteredTasks;
  }

  private StructLikeSet collectGroupingKeys(Iterable<ScanTaskGroup<T>> taskGroupIterable) {
    StructLikeSet keys = StructLikeSet.create(groupingKeyType());

    for (ScanTaskGroup<T> taskGroup : taskGroupIterable) {
      keys.add(taskGroup.groupingKey());
    }

    return keys;
  }
}
