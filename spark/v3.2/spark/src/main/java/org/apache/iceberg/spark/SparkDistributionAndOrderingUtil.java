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

import java.util.List;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.transforms.SortOrderVisitor;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command;

import static org.apache.iceberg.DistributionMode.HASH;
import static org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.DELETE;

public class SparkDistributionAndOrderingUtil {

  private static final NamedReference FILE_PATH = Expressions.column(MetadataColumns.FILE_PATH.name());
  private static final NamedReference ROW_POSITION = Expressions.column(MetadataColumns.ROW_POSITION.name());

  private static final SortOrder FILE_PATH_ORDER = Expressions.sort(FILE_PATH, SortDirection.ASCENDING);
  private static final SortOrder ROW_POSITION_ORDER = Expressions.sort(ROW_POSITION, SortDirection.ASCENDING);

  private SparkDistributionAndOrderingUtil() {
  }

  public static Distribution buildRequiredDistribution(Table table, DistributionMode distributionMode) {
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        return Distributions.clustered(Spark3Util.toTransforms(table.spec()));

      case RANGE:
        org.apache.iceberg.SortOrder requiredSortOrder = SortOrderUtil.buildSortOrder(table);
        return Distributions.ordered(convert(requiredSortOrder));

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + distributionMode);
    }
  }

  public static SortOrder[] buildRequiredOrdering(Table table, Distribution distribution) {
    if (distribution instanceof OrderedDistribution) {
      OrderedDistribution orderedDistribution = (OrderedDistribution) distribution;
      return orderedDistribution.ordering();

    } else {
      org.apache.iceberg.SortOrder requiredSortOrder = SortOrderUtil.buildSortOrder(table);
      return convert(requiredSortOrder);
    }
  }

  public static Distribution buildCopyOnWriteDistribution(Table table, Command command,
                                                          DistributionMode distributionMode) {
    if (command == DELETE && distributionMode == HASH) {
      Expression[] clustering = new Expression[]{FILE_PATH};
      return Distributions.clustered(clustering);
    } else {
      return buildRequiredDistribution(table, distributionMode);
    }
  }

  public static SortOrder[] buildCopyOnWriteOrdering(Table table, Command command, Distribution distribution) {
    if (command == DELETE && distribution instanceof ClusteredDistribution) {
      return new SortOrder[]{FILE_PATH_ORDER, ROW_POSITION_ORDER};
    } else {
      return buildRequiredOrdering(table, distribution);
    }
  }

  public static SortOrder[] convert(org.apache.iceberg.SortOrder sortOrder) {
    List<SortOrder> converted = SortOrderVisitor.visit(sortOrder, new SortOrderToSpark(sortOrder.schema()));
    return converted.toArray(new SortOrder[0]);
  }
}
