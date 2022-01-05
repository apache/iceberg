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

package org.apache.iceberg.flink.sink;

import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

/**
 * Create a {@link KeySelector} to shuffle by equality fields, to ensure same equality fields record will be emitted to
 * same writer. That can prevent create duplicate record when insert and delete one row which have same equality field
 * values on different writer in one transaction, and guarantee pos-delete will take effect.
 */
class EqualityFieldKeySelector extends BaseKeySelector<RowData, StructLikeWrapper> {

  private final Schema schema;
  private final Schema deleteSchema;

  private transient StructProjection structProjection;
  private transient StructLikeWrapper structLikeWrapper;

  EqualityFieldKeySelector(List<Integer> equalityFieldIds, Schema schema, RowType flinkSchema) {
    super(schema, flinkSchema);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
  }

  /**
   * Construct the {@link StructProjection} lazily because it is not serializable.
   */
  protected StructProjection lazyStructProjection() {
    if (structProjection == null) {
      structProjection = StructProjection.create(schema, deleteSchema);
    }
    return structProjection;
  }

  /**
   * Construct the {@link StructLikeWrapper} lazily because it is not serializable.
   */
  protected StructLikeWrapper lazyStructLikeWrapper() {
    if (structLikeWrapper == null) {
      structLikeWrapper = StructLikeWrapper.forType(deleteSchema.asStruct());
    }
    return structLikeWrapper;
  }

  @Override
  public StructLikeWrapper getKey(RowData row) {
    return lazyStructLikeWrapper().set(lazyStructProjection().wrap(lazyRowDataWrapper().wrap(row)));
  }
}
