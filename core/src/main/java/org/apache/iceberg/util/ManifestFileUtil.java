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

import com.google.common.collect.Lists;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class ManifestFileUtil {
  private ManifestFileUtil() {
  }

  private static class FieldSummary<T> {
    private final Comparator<T> comparator;
    private final Class<T> javaClass;
    private final T lowerBound;
    private final T upperBound;
    private final boolean containsNull;

    @SuppressWarnings("unchecked")
    FieldSummary(Type.PrimitiveType primitive, ManifestFile.PartitionFieldSummary summary) {
      this.comparator = Comparators.forType(primitive);
      this.javaClass = (Class<T>) primitive.typeId().javaClass();
      this.lowerBound = Conversions.fromByteBuffer(primitive, summary.lowerBound());
      this.upperBound = Conversions.fromByteBuffer(primitive, summary.upperBound());
      this.containsNull = summary.containsNull();
    }

    boolean canContain(Object value) {
      if (value == null) {
        return containsNull;
      }

      // if lower bound is null, then there are no non-null values
      if (lowerBound == null) {
        // the value is non-null, so it cannot match
        return false;
      }

      if (!javaClass.isInstance(value)) {
        return false;
      }

      T typedValue = javaClass.cast(value);

      if (comparator.compare(typedValue, lowerBound) < 0) {
        return false;
      }

      if (comparator.compare(typedValue, upperBound) > 0) {
        return false;
      }

      return true;
    }
  }

  private static boolean canContain(List<FieldSummary<?>> summaries, StructLike struct) {
    if (struct.size() != summaries.size()) {
      return false;
    }

    // if any value is not contained, the struct is not contained and this can return early
    for (int pos = 0; pos < summaries.size(); pos += 1) {
      Object value = struct.get(pos, Object.class);
      if (!summaries.get(pos).canContain(value)) {
        return false;
      }
    }

    return true;
  }

  public static boolean canContainAny(ManifestFile manifest,
                                      Iterable<StructLike> partitions,
                                      Function<Integer, PartitionSpec> specLookup) {
    if (manifest.partitions() == null) {
      return true;
    }

    Types.StructType partitionType = specLookup.apply(manifest.partitionSpecId()).partitionType();
    List<ManifestFile.PartitionFieldSummary> fieldSummaries = manifest.partitions();
    List<Types.NestedField> fields = partitionType.fields();

    List<FieldSummary<?>> summaries = Lists.newArrayListWithExpectedSize(fieldSummaries.size());
    for (int pos = 0; pos < fieldSummaries.size(); pos += 1) {
      Type.PrimitiveType primitive = fields.get(pos).type().asPrimitiveType();
      summaries.add(new FieldSummary<>(primitive, fieldSummaries.get(pos)));
    }

    for (StructLike partition : partitions) {
      if (canContain(summaries, partition)) {
        return true;
      }
    }

    return false;
  }
}
