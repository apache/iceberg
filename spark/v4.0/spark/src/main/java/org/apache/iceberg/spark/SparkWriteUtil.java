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
package org.apache.iceberg.spark;

import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.DELETE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE;
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE;

import java.util.Arrays;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;

/**
 * A utility that contains helper methods for working with Spark writes.
 *
 * <p>Note it is an evolving internal API that is subject to change even in minor releases.
 */
public class SparkWriteUtil {

  private static final NamedReference SPEC_ID = ref(MetadataColumns.SPEC_ID);
  private static final NamedReference PARTITION = ref(MetadataColumns.PARTITION_COLUMN_NAME);
  private static final NamedReference FILE_PATH = ref(MetadataColumns.FILE_PATH);
  private static final NamedReference ROW_POSITION = ref(MetadataColumns.ROW_POSITION);

  private static final Expression[] FILE_CLUSTERING = clusterBy(FILE_PATH);
  private static final Expression[] PARTITION_CLUSTERING = clusterBy(SPEC_ID, PARTITION);
  private static final Expression[] PARTITION_FILE_CLUSTERING =
      clusterBy(SPEC_ID, PARTITION, FILE_PATH);

  private static final SortOrder[] EMPTY_ORDERING = new SortOrder[0];
  private static final SortOrder[] EXISTING_ROW_ORDERING = orderBy(FILE_PATH, ROW_POSITION);
  private static final SortOrder[] PARTITION_ORDERING = orderBy(SPEC_ID, PARTITION);
  private static final SortOrder[] PARTITION_FILE_ORDERING = orderBy(SPEC_ID, PARTITION, FILE_PATH);
  private static final SortOrder[] POSITION_DELETE_ORDERING =
      orderBy(SPEC_ID, PARTITION, FILE_PATH, ROW_POSITION);

  private SparkWriteUtil() {}

  /** Builds requirements for batch and micro-batch writes such as append or overwrite. */
  public static SparkWriteRequirements writeRequirements(
      Table table, DistributionMode mode, boolean fanoutEnabled, long advisoryPartitionSize) {

    Distribution distribution = writeDistribution(table, mode);
    SortOrder[] ordering = writeOrdering(table, fanoutEnabled);
    return new SparkWriteRequirements(distribution, ordering, advisoryPartitionSize);
  }

  private static Distribution writeDistribution(Table table, DistributionMode mode) {
    switch (mode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        return Distributions.clustered(clustering(table));

      case RANGE:
        return Distributions.ordered(ordering(table));

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + mode);
    }
  }

  /** Builds requirements for copy-on-write DELETE, UPDATE, MERGE operations. */
  public static SparkWriteRequirements copyOnWriteRequirements(
      Table table,
      Command command,
      DistributionMode mode,
      boolean fanoutEnabled,
      long advisoryPartitionSize) {

    if (command == DELETE || command == UPDATE) {
      Distribution distribution = copyOnWriteDeleteUpdateDistribution(table, mode);
      SortOrder[] ordering = writeOrdering(table, fanoutEnabled);
      return new SparkWriteRequirements(distribution, ordering, advisoryPartitionSize);
    } else {
      return writeRequirements(table, mode, fanoutEnabled, advisoryPartitionSize);
    }
  }

  private static Distribution copyOnWriteDeleteUpdateDistribution(
      Table table, DistributionMode mode) {

    switch (mode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        if (table.spec().isPartitioned()) {
          return Distributions.clustered(clustering(table));
        } else {
          return Distributions.clustered(FILE_CLUSTERING);
        }

      case RANGE:
        if (table.spec().isPartitioned() || table.sortOrder().isSorted()) {
          return Distributions.ordered(ordering(table));
        } else {
          return Distributions.ordered(EXISTING_ROW_ORDERING);
        }

      default:
        throw new IllegalArgumentException("Unexpected distribution mode: " + mode);
    }
  }

  /** Builds requirements for merge-on-read DELETE, UPDATE, MERGE operations. */
  public static SparkWriteRequirements positionDeltaRequirements(
      Table table,
      Command command,
      DistributionMode mode,
      boolean fanoutEnabled,
      long advisoryPartitionSize) {

    if (command == UPDATE || command == MERGE) {
      Distribution distribution = positionDeltaUpdateMergeDistribution(table, mode);
      SortOrder[] ordering = positionDeltaUpdateMergeOrdering(table, fanoutEnabled);
      return new SparkWriteRequirements(distribution, ordering, advisoryPartitionSize);
    } else {
      Distribution distribution = positionDeltaDeleteDistribution(table, mode);
      SortOrder[] ordering = fanoutEnabled ? EMPTY_ORDERING : POSITION_DELETE_ORDERING;
      return new SparkWriteRequirements(distribution, ordering, advisoryPartitionSize);
    }
  }

  private static Distribution positionDeltaUpdateMergeDistribution(
      Table table, DistributionMode mode) {

    switch (mode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        if (table.spec().isUnpartitioned()) {
          return Distributions.clustered(concat(PARTITION_FILE_CLUSTERING, clustering(table)));
        } else {
          return Distributions.clustered(concat(PARTITION_CLUSTERING, clustering(table)));
        }

      case RANGE:
        if (table.spec().isUnpartitioned()) {
          return Distributions.ordered(concat(PARTITION_FILE_ORDERING, ordering(table)));
        } else {
          return Distributions.ordered(concat(PARTITION_ORDERING, ordering(table)));
        }

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + mode);
    }
  }

  private static SortOrder[] positionDeltaUpdateMergeOrdering(Table table, boolean fanoutEnabled) {
    if (fanoutEnabled && table.sortOrder().isUnsorted()) {
      return EMPTY_ORDERING;
    } else {
      return concat(POSITION_DELETE_ORDERING, ordering(table));
    }
  }

  private static Distribution positionDeltaDeleteDistribution(Table table, DistributionMode mode) {
    switch (mode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        if (table.spec().isUnpartitioned()) {
          return Distributions.clustered(PARTITION_FILE_CLUSTERING);
        } else {
          return Distributions.clustered(PARTITION_CLUSTERING);
        }

      case RANGE:
        if (table.spec().isUnpartitioned()) {
          return Distributions.ordered(PARTITION_FILE_ORDERING);
        } else {
          return Distributions.ordered(PARTITION_ORDERING);
        }

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + mode);
    }
  }

  // a local ordering within a task is beneficial in two cases:
  // - there is a defined table sort order, so it is clear how the data should be ordered
  // - the table is partitioned and fanout writers are disabled,
  //   so records for one partition must be co-located within a task
  private static SortOrder[] writeOrdering(Table table, boolean fanoutEnabled) {
    if (fanoutEnabled && table.sortOrder().isUnsorted()) {
      return EMPTY_ORDERING;
    } else {
      return ordering(table);
    }
  }

  private static Expression[] clustering(Table table) {
    return Spark3Util.toTransforms(table.spec());
  }

  private static SortOrder[] ordering(Table table) {
    return Spark3Util.toOrdering(SortOrderUtil.buildSortOrder(table));
  }

  private static Expression[] concat(Expression[] clustering, Expression... otherClustering) {
    return ObjectArrays.concat(clustering, otherClustering, Expression.class);
  }

  private static SortOrder[] concat(SortOrder[] ordering, SortOrder... otherOrdering) {
    return ObjectArrays.concat(ordering, otherOrdering, SortOrder.class);
  }

  private static NamedReference ref(Types.NestedField field) {
    return Expressions.column(field.name());
  }

  private static NamedReference ref(String name) {
    return Expressions.column(name);
  }

  private static Expression[] clusterBy(Expression... exprs) {
    return exprs;
  }

  private static SortOrder[] orderBy(Expression... exprs) {
    return Arrays.stream(exprs).map(SparkWriteUtil::sort).toArray(SortOrder[]::new);
  }

  private static SortOrder sort(Expression expr) {
    return Expressions.sort(expr, SortDirection.ASCENDING);
  }
}
