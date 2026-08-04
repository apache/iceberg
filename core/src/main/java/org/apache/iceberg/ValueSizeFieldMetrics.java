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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Field-level metrics for tracking the average size of variable-length values. */
public class ValueSizeFieldMetrics extends FieldMetrics<Object> {

  private ValueSizeFieldMetrics(int id, long valueCount, Integer avgValueSizeInBytes) {
    super(id, valueCount, 0L, -1L, null, null, null, avgValueSizeInBytes);
  }

  public static class Builder {
    private final int id;
    private long valueCount = 0;
    private long totalValueSizeInBytes = 0;

    public Builder(int id) {
      this.id = id;
    }

    public void addValueSize(int sizeInBytes) {
      Preconditions.checkArgument(sizeInBytes >= 0, "Invalid value size: %s", sizeInBytes);
      this.valueCount += 1;
      this.totalValueSizeInBytes += sizeInBytes;
    }

    public ValueSizeFieldMetrics build() {
      Integer avgValueSizeInBytes =
          valueCount > 0 ? Math.toIntExact(totalValueSizeInBytes / valueCount) : null;
      return new ValueSizeFieldMetrics(id, valueCount, avgValueSizeInBytes);
    }
  }
}
