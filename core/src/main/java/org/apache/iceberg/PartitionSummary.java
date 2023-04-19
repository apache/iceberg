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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.NaNUtil;

class PartitionSummary {
  private final PartitionFieldStats<?>[] fields;
  private final Class<?>[] javaClasses;

  PartitionSummary(PartitionSpec spec) {
    this.javaClasses = spec.javaClasses();
    this.fields = new PartitionFieldStats[javaClasses.length];
    List<Types.NestedField> partitionFields = spec.partitionType().fields();
    for (int i = 0; i < fields.length; i += 1) {
      this.fields[i] = new PartitionFieldStats<>(partitionFields.get(i).type());
    }
  }

  List<PartitionFieldSummary> summaries() {
    return Arrays.stream(fields).map(PartitionFieldStats::toSummary).collect(Collectors.toList());
  }

  public void update(StructLike partitionKey) {
    updateFields(partitionKey);
  }

  @SuppressWarnings("unchecked")
  private <T> void updateFields(StructLike key) {
    for (int i = 0; i < javaClasses.length; i += 1) {
      PartitionFieldStats<T> stats = (PartitionFieldStats<T>) fields[i];
      Class<T> javaClass = (Class<T>) javaClasses[i];
      stats.update(key.get(i, javaClass));
    }
  }

  private static class PartitionFieldStats<T> {
    private final Type type;
    private final Comparator<T> comparator;

    private boolean containsNull = false;
    private boolean containsNaN = false;
    private T min = null;
    private T max = null;

    private PartitionFieldStats(Type type) {
      this.type = type;
      this.comparator = Comparators.forType(type.asPrimitiveType());
    }

    public PartitionFieldSummary toSummary() {
      return new GenericPartitionFieldSummary(
          containsNull,
          containsNaN,
          min != null ? Conversions.toByteBuffer(type, min) : null,
          max != null ? Conversions.toByteBuffer(type, max) : null);
    }

    void update(T value) {
      if (value == null) {
        this.containsNull = true;
      } else if (NaNUtil.isNaN(value)) {
        this.containsNaN = true;
      } else if (min == null) {
        this.min = value;
        this.max = value;
      } else {
        if (comparator.compare(value, min) < 0) {
          this.min = value;
        }
        if (comparator.compare(max, value) < 0) {
          this.max = value;
        }
      }
    }
  }
}
