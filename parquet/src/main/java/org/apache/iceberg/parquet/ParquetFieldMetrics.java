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

package org.apache.iceberg.parquet;

import java.nio.ByteBuffer;
import org.apache.iceberg.FieldMetrics;

/**
 * Iceberg internally tracked field level metrics, used by Parquet writer only.
 * <p>
 * Parquet keeps track of most metrics in its footer, and only NaN counter is actually tracked by writers.
 * This wrapper ensures that metrics not being updated by Parquet writers will not be incorrectly used, by throwing
 * exceptions when they are accessed.
 */
public class ParquetFieldMetrics extends FieldMetrics {

  /**
   * Constructor for creating a Parquet-specific FieldMetrics.
   * @param id field id being tracked by the writer
   * @param nanValueCount number of NaN values, will only be non-0 for double or float field.
   */
  public ParquetFieldMetrics(int id,
                             long nanValueCount) {
    super(id, 0L, 0L, nanValueCount, null, null);
  }

  @Override
  public long valueCount() {
    throw new IllegalStateException(
        "Shouldn't access valueCount() within ParquetFieldMetrics, as this metric is tracked by Parquet footer. ");
  }

  @Override
  public long nullValueCount() {
    throw new IllegalStateException(
        "Shouldn't access nullValueCount() within ParquetFieldMetrics, as this metric is tracked by Parquet footer. ");
  }

  @Override
  public ByteBuffer lowerBound() {
    throw new IllegalStateException(
        "Shouldn't access lowerBound() within ParquetFieldMetrics, as this metric is tracked by Parquet footer. ");
  }

  @Override
  public ByteBuffer upperBound() {
    throw new IllegalStateException(
        "Shouldn't access upperBound() within ParquetFieldMetrics, as this metric is tracked by Parquet footer. ");
  }
}
