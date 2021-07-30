/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.sink;

import java.lang.reflect.Array;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Create a {@link KeySelector} to shuffle by equality fields, to ensure same equality fields record will be emit to
 * same writer. That can prevent create duplicate record when insert and delete one row which have same equality field
 * values on different writer in one transaction, and guarantee pos-delete will take effect.
 */
class EqualityFieldKeySelector extends BaseKeySelector<RowData, String> {

  private static final String SEPARATOR = "-";
  private final Integer keySize;
  private final Accessor<StructLike>[] accessors;

  @SuppressWarnings("unchecked")
  EqualityFieldKeySelector(List<Integer> equalityFieldIds, Schema schema, RowType flinkSchema) {
    super(schema, flinkSchema);
    this.keySize = equalityFieldIds.size();
    this.accessors = (Accessor<StructLike>[]) Array.newInstance(Accessor.class, keySize);

    for (int i = 0; i < keySize; i++) {
      Accessor<StructLike> accessor = schema.accessorForField(equalityFieldIds.get(i));
      Preconditions.checkArgument(accessor != null,
          "Cannot build accessor for field: " + schema.findField(equalityFieldIds.get(i)));
      accessors[i] = accessor;
    }
  }

  @Override
  public String getKey(RowData row) {
    String[] values = new String[keySize];
    for (int i = 0; i < keySize; i++) {
      values[i] = accessors[i].get(lazyRowDataWrapper().wrap(row)).toString();
    }
    return String.join(SEPARATOR, values);
  }
}
