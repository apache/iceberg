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

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.transforms.SortOrderVisitor;

class CopySortOrderFields implements SortOrderVisitor<Void> {
  private final SortOrder.Builder builder;

  CopySortOrderFields(SortOrder.Builder builder) {
    this.builder = builder;
  }

  @Override
  public Void field(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(sourceName, nullOrder);
    } else {
      builder.desc(sourceName, nullOrder);
    }
    return null;
  }

  @Override
  public Void bucket(String sourceName, int sourceId, int numBuckets, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(Expressions.bucket(sourceName, numBuckets), nullOrder);
    } else {
      builder.desc(Expressions.bucket(sourceName, numBuckets), nullOrder);
    }
    return null;
  }

  @Override
  public Void truncate(String sourceName, int sourceId, int width, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(Expressions.truncate(sourceName, width), nullOrder);
    } else {
      builder.desc(Expressions.truncate(sourceName, width), nullOrder);
    }
    return null;
  }

  @Override
  public Void year(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(Expressions.year(sourceName), nullOrder);
    } else {
      builder.desc(Expressions.year(sourceName), nullOrder);
    }
    return null;
  }

  @Override
  public Void month(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(Expressions.month(sourceName), nullOrder);
    } else {
      builder.desc(Expressions.month(sourceName), nullOrder);
    }
    return null;
  }

  @Override
  public Void day(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(Expressions.day(sourceName), nullOrder);
    } else {
      builder.desc(Expressions.day(sourceName), nullOrder);
    }
    return null;
  }

  @Override
  public Void hour(String sourceName, int sourceId, SortDirection direction, NullOrder nullOrder) {
    if (direction == SortDirection.ASC) {
      builder.asc(Expressions.hour(sourceName), nullOrder);
    } else {
      builder.desc(Expressions.hour(sourceName), nullOrder);
    }
    return null;
  }
}
