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
package org.apache.iceberg.util;

import java.util.List;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for deciding whether aggregates can be answered from file-level metrics for aggregate push
 * down.
 */
public class AggregatePushDownUtil {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatePushDownUtil.class);

  private AggregatePushDownUtil() {}

  public static boolean metricsModeSupportsAggregatePushDown(
      Table table, List<BoundAggregate<?, ?>> aggregates) {
    MetricsConfig config = MetricsConfig.forTable(table);
    for (BoundAggregate<?, ?> aggregate : aggregates) {
      String colName = aggregate.columnName();
      if (!colName.equals("*")) {
        MetricsModes.MetricsMode mode = config.columnMode(colName);
        if (mode instanceof MetricsModes.None) {
          LOG.info("Skipping aggregate pushdown: no metrics for column {}", colName);
          return false;
        } else if (mode instanceof MetricsModes.Counts) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: cannot produce min or max from count for column {}",
                colName);
            return false;
          }
        } else if (aggregate.type().typeId() == Type.TypeID.STRING
            || aggregate.type().typeId() == Type.TypeID.BINARY) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: cannot produce min or max from truncated values for column {}",
                colName);
            return false;
          }
        }
      }
    }

    return true;
  }
}
