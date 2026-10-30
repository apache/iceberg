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

import java.util.Map;
import java.util.OptionalLong;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;

class Stats implements Statistics {
  private final OptionalLong sizeInBytes;
  private final OptionalLong numRows;
  private final Map<NamedReference, ColumnStatistics> colstats;

  Stats(long sizeInBytes, long numRows, Map<NamedReference, ColumnStatistics> colstats) {
    this.sizeInBytes = OptionalLong.of(sizeInBytes);
    this.numRows = OptionalLong.of(numRows);
    this.colstats = colstats;
  }

  @Override
  public OptionalLong sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public OptionalLong numRows() {
    return numRows;
  }

  @Override
  public Map<NamedReference, ColumnStatistics> columnStats() {
    return colstats;
  }
}
