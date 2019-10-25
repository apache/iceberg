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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.SortField.Direction;
import org.apache.iceberg.SortField.NullOrder;
import org.apache.iceberg.SortTransforms.SortTransform;
import org.apache.iceberg.types.Types;

// TODO: toString()
public class SortOrder implements Serializable {

  private static final SortOrder UNSORTED_ORDER = new SortOrder(new Schema(), 0, ImmutableList.of());

  private final Schema schema;
  private final int orderId;
  private final SortField[] fields;
  private transient volatile List<SortField> fieldList;

  public static SortOrder unsorted() {
    return UNSORTED_ORDER;
  }

  private SortOrder(Schema schema, int orderId, List<SortField> fields) {
    this.schema = schema;
    this.orderId = orderId;
    this.fields = new SortField[fields.size()];
    for (int fieldIndex = 0; fieldIndex < this.fields.length; fieldIndex++) {
      this.fields[fieldIndex] = fields.get(fieldIndex);
    }
  }

  public Schema schema() {
    return schema;
  }

  public int orderId() {
    return orderId;
  }

  public List<SortField> fields() {
    return lazyFieldList();
  }

  public boolean satisfies(SortOrder anotherSortOrder) {
    if (anotherSortOrder.fields.length == 0) {
      return true;
    }

    if (anotherSortOrder.fields.length > fields.length) {
      return false;
    }

    for (int fieldIndex = 0; fieldIndex < anotherSortOrder.fields.length; fieldIndex++) {
      SortField anotherField = anotherSortOrder.fields[fieldIndex];
      SortField field = fields[fieldIndex];
      if (!field.semanticEquals(anotherField)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    SortOrder that = (SortOrder) obj;
    return orderId == that.orderId && Arrays.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(orderId, Arrays.hashCode(fields));
  }

  private List<SortField> lazyFieldList() {
    if (fieldList == null) {
      synchronized (this) {
        if (fieldList == null) {
          this.fieldList = ImmutableList.copyOf(fields);
        }
      }
    }
    return fieldList;
  }

  public static Builder builderFor(Schema schema) {
    return new Builder(schema);
  }

  // TODO: add validation
  public static class Builder {
    private final Schema schema;
    private final List<SortField> fields = Lists.newArrayList();
    private int orderId = 0;

    private Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder natural(String columnName) {
      return natural(columnName, Direction.ASC, NullOrder.NULLS_FIRST);
    }

    public Builder natural(String columnName, Direction direction, NullOrder nullOrder) {
      Types.NestedField column = findSourceColumn(columnName);
      SortTransform transform = SortTransforms.IdentityTransform.get();
      SortField field = new SortField(columnName, direction, nullOrder, transform, new int[]{column.fieldId()});
      fields.add(field);
      return this;
    }

    public Builder withOrderId(int newOrderId) {
      this.orderId = newOrderId;
      return this;
    }

    public SortOrder build() {
      return new SortOrder(schema, orderId, fields);
    }

    Builder add(String name, Direction direction, NullOrder nullOrder, SortTransform transform, int[] sourceIds) {
      SortField field = new SortField(name, direction, nullOrder, transform, sourceIds);
      fields.add(field);
      return this;
    }

    private Types.NestedField findSourceColumn(String sourceName) {
      Types.NestedField sourceColumn = schema.findField(sourceName);
      Preconditions.checkArgument(sourceColumn != null, "Cannot find source column: %s", sourceName);
      return sourceColumn;
    }
  }
}
