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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class UnboundSortOrder {
  private static final UnboundSortOrder UNSORTED_ORDER =
      new UnboundSortOrder(0, Collections.emptyList());

  private final int orderId;
  private final List<UnboundSortField> fields;

  private UnboundSortOrder(int orderId, List<UnboundSortField> fields) {
    this.orderId = orderId;
    this.fields = fields;
  }

  public SortOrder bind(Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(orderId);

    for (UnboundSortField field : fields) {
      builder.addSortField(
          field.transformAsString, field.sourceId, field.direction, field.nullOrder);
    }

    return builder.build();
  }

  SortOrder bindUnchecked(Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema).withOrderId(orderId);

    for (UnboundSortField field : fields) {
      builder.addSortField(
          field.transformAsString, field.sourceId, field.direction, field.nullOrder);
    }

    return builder.buildUnchecked();
  }

  int orderId() {
    return orderId;
  }

  List<UnboundSortField> fields() {
    return fields;
  }

  /**
   * Creates a new {@link SortOrder.Builder sort order builder} for unbound sort orders.
   *
   * @return a sort order builder
   */
  static Builder builder() {
    return new Builder();
  }

  /**
   * A builder used to create {@link UnboundSortOrder unbound sort orders}.
   *
   * <p>Call {@link #builder()} to create a new builder.
   */
  static class Builder {
    private final List<UnboundSortField> fields = Lists.newArrayList();
    private Integer orderId = null;

    private Builder() {}

    Builder withOrderId(int newOrderId) {
      this.orderId = newOrderId;
      return this;
    }

    Builder addSortField(
        String transformAsString, int sourceId, SortDirection direction, NullOrder nullOrder) {
      fields.add(new UnboundSortField(transformAsString, sourceId, direction, nullOrder));
      return this;
    }

    UnboundSortOrder build() {
      if (fields.isEmpty()) {
        if (orderId != null && orderId != 0) {
          throw new IllegalArgumentException("Unsorted order ID must be 0");
        }
        return UNSORTED_ORDER;
      }

      if (orderId != null && orderId == 0) {
        throw new IllegalArgumentException("Sort order ID 0 is reserved for unsorted order");
      }

      // default ID to 1 as 0 is reserved for unsorted order
      int actualOrderId = orderId != null ? orderId : 1;
      return new UnboundSortOrder(actualOrderId, fields);
    }
  }

  static class UnboundSortField {
    private final String transformAsString;
    private final int sourceId;
    private final SortDirection direction;
    private final NullOrder nullOrder;

    private UnboundSortField(
        String transformAsString, int sourceId, SortDirection direction, NullOrder nullOrder) {
      this.transformAsString = transformAsString;
      this.sourceId = sourceId;
      this.direction = direction;
      this.nullOrder = nullOrder;
    }

    public String transformAsString() {
      return transformAsString;
    }

    public int sourceId() {
      return sourceId;
    }

    public SortDirection direction() {
      return direction;
    }

    public NullOrder nullOrder() {
      return nullOrder;
    }
  }
}
