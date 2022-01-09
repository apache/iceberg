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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.ByteBuffers;

/**
 * Create a {@link KeySelector} to shuffle by equality fields, to ensure same equality fields record will be emitted to
 * same writer. That can prevent create duplicate record when insert and delete one row which have same equality field
 * values on different writer in one transaction, and guarantee pos-delete will take effect.
 */
class EqualityFieldKeySelector extends BaseKeySelector<RowData, ByteBuffer> {

  private final Accessor<StructLike>[] accessors;

  @SuppressWarnings("unchecked")
  EqualityFieldKeySelector(List<Integer> equalityFieldIds, Schema schema, RowType flinkSchema) {
    super(schema, flinkSchema);

    int size = equalityFieldIds.size();
    this.accessors = (Accessor<StructLike>[]) Array.newInstance(Accessor.class, size);
    for (int i = 0; i < size; i++) {
      Accessor<StructLike> accessor = schema.accessorForField(equalityFieldIds.get(i));
      Preconditions.checkArgument(accessor != null,
          "Cannot build accessor for field: {}", schema.findField(equalityFieldIds.get(i)));

      accessors[i] = accessor;
    }
  }

  @Override
  public ByteBuffer getKey(RowData row) throws Exception {
    StructLike record = lazyRowDataWrapper().wrap(row);
    try (ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(binaryOut)) {
      for (Accessor<StructLike> accessor : accessors) {
        ByteBuffer buffer = Conversions.toByteBuffer(accessor.type(), accessor.get(record));
        if (buffer != null) {
          out.write(ByteBuffers.toByteArray(buffer));
        }
      }
      return ByteBuffer.wrap(binaryOut.toByteArray());
    }
  }
}
