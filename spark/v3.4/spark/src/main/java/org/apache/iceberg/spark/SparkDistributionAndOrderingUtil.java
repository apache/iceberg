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
import static org.apache.spark.sql.connector.write.RowLevelOperation.Command.UPDATE;

import java.util.List;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.transforms.SortOrderVisitor;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;

public class SparkDistributionAndOrderingUtil {

  private static final NamedReference SPEC_ID = Expressions.column(MetadataColumns.SPEC_ID.name());
  private static final NamedReference PARTITION =
      Expressions.column(MetadataColumns.PARTITION_COLUMN_NAME);
  private static final NamedReference FILE_PATH =
      Expressions.column(MetadataColumns.FILE_PATH.name());
  private static final NamedReference ROW_POSITION =
      Expressions.column(MetadataColumns.ROW_POSITION.name());

  private static final SortOrder SPEC_ID_ORDER = Expressions.sort(SPEC_ID, SortDirection.ASCENDING);
  private static final SortOrder PARTITION_ORDER =
      Expressions.sort(PARTITION, SortDirection.ASCENDING);
  private static final SortOrder FILE_PATH_ORDER =
      Expressions.sort(FILE_PATH, SortDirection.ASCENDING);
  private static final SortOrder ROW_POSITION_ORDER =
      Expressions.sort(ROW_POSITION, SortDirection.ASCENDING);

  private static final SortOrder[] EXISTING_FILE_ORDERING =
      new SortOrder[] {FILE_PATH_ORDER, ROW_POSITION_ORDER};
  private static final SortOrder[] POSITION_DELETE_ORDERING =
      new SortOrder[] {SPEC_ID_ORDER, PARTITION_ORDER, FILE_PATH_ORDER, ROW_POSITION_ORDER};

  private SparkDistributionAndOrderingUtil() {}

  public static Distribution buildRequiredDistribution(
      Table table, DistributionMode distributionMode) {
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        return Distributions.clustered(Spark3Util.toTransforms(table.spec()));

      case RANGE:
        return Distributions.ordered(buildTableOrdering(table));

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + distributionMode);
    }
  }

  public static SortOrder[] buildRequiredOrdering(Table table, Distribution distribution) {
    if (distribution instanceof OrderedDistribution) {
      OrderedDistribution orderedDistribution = (OrderedDistribution) distribution;
      return orderedDistribution.ordering();
    } else {
      return buildTableOrdering(table);
    }
  }

  public static Distribution buildCopyOnWriteDistribution(
      Table table, Command command, DistributionMode distributionMode) {
    if (command == DELETE || command == UPDATE) {
      return buildCopyOnWriteDeleteUpdateDistribution(table, distributionMode);
    } else {
      return buildRequiredDistribution(table, distributionMode);
    }
  }

  private static Distribution buildCopyOnWriteDeleteUpdateDistribution(
      Table table, DistributionMode distributionMode) {
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        Expression[] clustering = new Expression[] {FILE_PATH};
        return Distributions.clustered(clustering);

      case RANGE:
        SortOrder[] tableOrdering = buildTableOrdering(table);
        if (table.sortOrder().isSorted()) {
          return Distributions.ordered(tableOrdering);
        } else {
          SortOrder[] ordering =
              ObjectArrays.concat(tableOrdering, EXISTING_FILE_ORDERING, SortOrder.class);
          return Distributions.ordered(ordering);
        }

      default:
        throw new IllegalArgumentException("Unexpected distribution mode: " + distributionMode);
    }
  }

  public static SortOrder[] buildCopyOnWriteOrdering(
      Table table, Command command, Distribution distribution) {
    if (command == DELETE || command == UPDATE) {
      return buildCopyOnWriteDeleteUpdateOrdering(table, distribution);
    } else {
      return buildRequiredOrdering(table, distribution);
    }
  }

  private static SortOrder[] buildCopyOnWriteDeleteUpdateOrdering(
      Table table, Distribution distribution) {
    if (distribution instanceof UnspecifiedDistribution) {
      return buildTableOrdering(table);

    } else if (distribution instanceof ClusteredDistribution) {
      SortOrder[] tableOrdering = buildTableOrdering(table);
      if (table.sortOrder().isSorted()) {
        return tableOrdering;
      } else {
        return ObjectArrays.concat(tableOrdering, EXISTING_FILE_ORDERING, SortOrder.class);
      }

    } else if (distribution instanceof OrderedDistribution) {
      OrderedDistribution orderedDistribution = (OrderedDistribution) distribution;
      return orderedDistribution.ordering();

    } else {
      throw new IllegalArgumentException(
          "Unexpected distribution type: " + distribution.getClass().getName());
    }
  }

  public static Distribution buildPositionDeltaDistribution(
      Table table, Command command, DistributionMode distributionMode) {
    if (command == DELETE || command == UPDATE) {
      return buildPositionDeleteUpdateDistribution(distributionMode);
    } else {
      return buildPositionMergeDistribution(table, distributionMode);
    }
  }

  private static Distribution buildPositionMergeDistribution(
      Table table, DistributionMode distributionMode) {
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        if (table.spec().isUnpartitioned()) {
          Expression[] clustering = new Expression[] {SPEC_ID, PARTITION, FILE_PATH};
          return Distributions.clustered(clustering);
        } else {
          Distribution dataDistribution = buildRequiredDistribution(table, distributionMode);
          Expression[] dataClustering = ((ClusteredDistribution) dataDistribution).clustering();
          Expression[] deleteClustering = new Expression[] {SPEC_ID, PARTITION};
          Expression[] clustering =
              ObjectArrays.concat(deleteClustering, dataClustering, Expression.class);
          return Distributions.clustered(clustering);
        }

      case RANGE:
        Distribution dataDistribution = buildRequiredDistribution(table, distributionMode);
        SortOrder[] dataOrdering = ((OrderedDistribution) dataDistribution).ordering();
        SortOrder[] deleteOrdering =
            new SortOrder[] {SPEC_ID_ORDER, PARTITION_ORDER, FILE_PATH_ORDER};
        SortOrder[] ordering = ObjectArrays.concat(deleteOrdering, dataOrdering, SortOrder.class);
        return Distributions.ordered(ordering);

      default:
        throw new IllegalArgumentException("Unexpected distribution mode: " + distributionMode);
    }
  }

  private static Distribution buildPositionDeleteUpdateDistribution(
      DistributionMode distributionMode) {
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        Expression[] clustering = new Expression[] {SPEC_ID, PARTITION};
        return Distributions.clustered(clustering);

      case RANGE:
        SortOrder[] ordering = new SortOrder[] {SPEC_ID_ORDER, PARTITION_ORDER, FILE_PATH_ORDER};
        return Distributions.ordered(ordering);

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + distributionMode);
    }
  }

  public static SortOrder[] buildPositionDeltaOrdering(Table table, Command command) {
    if (command == DELETE) {
      return POSITION_DELETE_ORDERING;
    } else {
      // all metadata columns like _spec_id, _file, _pos will be null for new data records
      SortOrder[] dataOrdering = buildTableOrdering(table);
      return ObjectArrays.concat(POSITION_DELETE_ORDERING, dataOrdering, SortOrder.class);
    }
  }

  public static SortOrder[] convert(org.apache.iceberg.SortOrder sortOrder) {
    List<SortOrder> converted =
        SortOrderVisitor.visit(sortOrder, new SortOrderToSpark(sortOrder.schema()));
    return converted.toArray(new SortOrder[0]);
  }

  private static SortOrder[] buildTableOrdering(Table table) {
    return convert(SortOrderUtil.buildSortOrder(table));
  }
}
