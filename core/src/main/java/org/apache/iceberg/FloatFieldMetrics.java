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
package org.apache.iceberg;

/**
 * Iceberg internally tracked field level metrics, used by Parquet and ORC writers only.
 *
 * <p>Parquet/ORC keeps track of most metrics in file statistics, and only NaN counter is actually
 * tracked by writers. This wrapper ensures that metrics not being updated by those writers will not
 * be incorrectly used, by throwing exceptions when they are accessed.
 */
public class FloatFieldMetrics extends FieldMetrics<Float> {

  private FloatFieldMetrics(
      int id, long valueCount, long nanValueCount, Float lowerBound, Float upperBound) {
    super(id, valueCount, 0L, nanValueCount, lowerBound, upperBound);
  }

  public Builder builderFor(int id) {
    return new Builder(id);
  }

  public static class Builder {
    private final int id;
    private long valueCount = 0;
    private long nanValueCount = 0;
    private float lowerBound = Float.POSITIVE_INFINITY;
    private float upperBound = Float.NEGATIVE_INFINITY;

    public Builder(int id) {
      this.id = id;
    }

    public void addValue(float value) {
      this.valueCount++;
      if (Float.isNaN(value)) {
        this.nanValueCount++;
      } else {
        if (Float.compare(value, lowerBound) < 0) {
          this.lowerBound = value;
        }
        if (Float.compare(value, upperBound) > 0) {
          this.upperBound = value;
        }
      }
    }

    public FloatFieldMetrics build() {
      boolean hasBound = valueCount - nanValueCount > 0;
      return new FloatFieldMetrics(
          id,
          valueCount,
          nanValueCount,
          hasBound ? lowerBound : null,
          hasBound ? upperBound : null);
    }
  }
}
