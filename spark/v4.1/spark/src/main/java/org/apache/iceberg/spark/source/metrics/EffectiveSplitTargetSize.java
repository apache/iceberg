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
package org.apache.iceberg.spark.source.metrics;

import org.apache.spark.sql.connector.metric.CustomMetric;

public class EffectiveSplitTargetSize implements CustomMetric {

  public static final String NAME = "effectiveSplitTargetSize";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "effective split target size (bytes)";
  }

  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    // the driver reports one value per scan, which is not additive
    long max = 0L;
    for (long taskMetric : taskMetrics) {
      max = Math.max(max, taskMetric);
    }

    return String.valueOf(max);
  }
}
