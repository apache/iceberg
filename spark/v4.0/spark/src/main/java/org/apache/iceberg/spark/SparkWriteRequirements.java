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

import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution;
import org.apache.spark.sql.connector.expressions.SortOrder;

/** A set of requirements such as distribution and ordering reported to Spark during writes. */
public class SparkWriteRequirements {

  public static final SparkWriteRequirements EMPTY =
      new SparkWriteRequirements(Distributions.unspecified(), new SortOrder[0], 0);

  private final Distribution distribution;
  private final SortOrder[] ordering;
  private final long advisoryPartitionSize;

  SparkWriteRequirements(
      Distribution distribution, SortOrder[] ordering, long advisoryPartitionSize) {
    this.distribution = distribution;
    this.ordering = ordering;
    this.advisoryPartitionSize = advisoryPartitionSize;
  }

  public Distribution distribution() {
    return distribution;
  }

  public SortOrder[] ordering() {
    return ordering;
  }

  public boolean hasOrdering() {
    return ordering.length != 0;
  }

  public long advisoryPartitionSize() {
    // Spark prohibits requesting a particular advisory partition size without distribution
    return distribution instanceof UnspecifiedDistribution ? 0 : advisoryPartitionSize;
  }
}
