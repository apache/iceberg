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
package org.apache.iceberg.expressions;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

public class CountNonNull<T> extends CountAggregate<T> {
  private final int fieldId;
  private final Types.NestedField field;

  protected CountNonNull(BoundTerm<T> term) {
    super(Operation.COUNT, term);
    this.field = term.ref().field();
    this.fieldId = field.fieldId();
  }

  @Override
  protected Long countFor(StructLike row) {
    return term().eval(row) != null ? 1L : 0L;
  }

  @Override
  protected boolean hasValue(DataFile file) {
    if (file.valueCounts() == null) {
      return false;
    }
    return file.valueCounts().containsKey(fieldId) && file.nullValueCounts().containsKey(fieldId);
  }

  @Override
  protected Long countFor(DataFile file) {
    return safeSubtract(
        safeGet(file.valueCounts(), fieldId), safeGet(file.nullValueCounts(), fieldId, 0L));
  }

  private Long safeSubtract(Long left, Long right) {
    if (left != null && right != null) {
      return left - right;
    }

    return null;
  }
}
