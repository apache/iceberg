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

import java.lang.reflect.Array;
import java.util.Comparator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SortOrderComparators {
  private SortOrderComparators() {}

  /** Compare structs with the specified sort order projection */
  public static Comparator<StructLike> forSchema(Schema schema, SortOrder sortOrder) {
    Preconditions.checkArgument(sortOrder.isSorted(), "Invalid sort order: unsorted");
    SortOrder.checkCompatibility(sortOrder, schema);
    return new SortOrderComparator(schema, sortOrder);
  }

  /** Util method to chain sort direction and null order to the original comparator. */
  private static Comparator<Object> sortFieldComparator(
      Comparator<Object> original, SortField sortField) {
    Comparator<Object> comparator = original;
    if (sortField == null) {
      return Comparators.nullsFirst().thenComparing(comparator);
    }

    if (sortField.direction() == SortDirection.DESC) {
      comparator = comparator.reversed();
    }

    if (sortField.nullOrder() == NullOrder.NULLS_FIRST) {
      comparator = Comparators.nullsFirst().thenComparing(comparator);
    } else if (sortField.nullOrder() == NullOrder.NULLS_LAST) {
      comparator = Comparators.nullsLast().thenComparing(comparator);
    }

    return comparator;
  }

  private static class SortOrderComparator implements Comparator<StructLike> {
    private final SortKey leftKey;
    private final SortKey rightKey;
    private final int size;
    private final Comparator<Object>[] comparators;
    private final Type[] transformResultTypes;

    private SortOrderComparator(Schema schema, SortOrder sortOrder) {
      this.leftKey = new SortKey(schema, sortOrder);
      this.rightKey = new SortKey(schema, sortOrder);
      this.size = sortOrder.fields().size();
      this.comparators = (Comparator<Object>[]) Array.newInstance(Comparator.class, size);
      this.transformResultTypes = (Type[]) Array.newInstance(Type.class, size);

      for (int i = 0; i < size; ++i) {
        SortField sortField = sortOrder.fields().get(i);
        Types.NestedField field = schema.findField(sortField.sourceId());
        Type transformResultType = sortField.transform().getResultType(field.type());
        Preconditions.checkArgument(
            transformResultType.isPrimitiveType(), "Invalid transform result type: non-primitive");
        transformResultTypes[i] = transformResultType;
        Comparator<Object> comparator = Comparators.forType(transformResultType.asPrimitiveType());
        comparators[i] = sortFieldComparator(comparator, sortField);
      }
    }

    @Override
    public int compare(StructLike left, StructLike right) {
      if (left == right) {
        return 0;
      }

      leftKey.wrap(left);
      rightKey.wrap(right);

      for (int i = 0; i < size; i += 1) {
        Class<?> valueClass = transformResultTypes[i].typeId().javaClass();
        int cmp = comparators[i].compare(leftKey.get(i, valueClass), rightKey.get(i, valueClass));
        if (cmp != 0) {
          return cmp;
        }
      }

      return 0;
    }
  }
}
