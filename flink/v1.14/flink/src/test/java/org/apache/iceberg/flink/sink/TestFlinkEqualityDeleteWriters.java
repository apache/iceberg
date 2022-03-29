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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.TestEqualityDeltaWriters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;

public class TestFlinkEqualityDeleteWriters extends TestEqualityDeltaWriters<RowData> {

  public TestFlinkEqualityDeleteWriters(FileFormat fileFormat) {
    super(fileFormat);
  }

  private boolean sameElements(List<Integer> left, List<Integer> right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    return ImmutableSet.copyOf(left).equals(ImmutableSet.copyOf(right));
  }

  @Override
  protected RowData toKey(List<Integer> keyFieldIds, Integer id, String data) {
    if (sameElements(fullKey(), keyFieldIds)) {
      return GenericRowData.of(id, data);
    } else if (sameElements(idKey(), keyFieldIds)) {
      return GenericRowData.of(id);
    } else if (sameElements(dataKey(), keyFieldIds)) {
      return GenericRowData.of(data);
    } else {
      throw new UnsupportedOperationException("Unknown equality field ids: " + keyFieldIds);
    }
  }

  @Override
  protected StructLikeSet toSet(Iterable<RowData> rows) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    for (RowData row : rows) {
      set.add(asStructLike(row));
    }
    return set;
  }

  @Override
  public StructLike asStructLike(RowData data) {
    RowType flinkType = FlinkSchemaUtil.convert(table.schema());
    RowDataWrapper wrapper = new RowDataWrapper(flinkType, table.schema().asStruct());

    return wrapper.wrap(data);
  }

  @Override
  public StructLike asStructLikeKey(List<Integer> keyFieldIds, RowData key) {
    Schema deleteSchema = TypeUtil.select(table.schema(), Sets.newHashSet(keyFieldIds));
    RowType keyType = FlinkSchemaUtil.convert(deleteSchema);
    RowDataWrapper wrapper = new RowDataWrapper(keyType, deleteSchema.asStruct());
    return wrapper.wrap(key);
  }

  @Override
  protected FileWriterFactory<RowData> newWriterFactory(
      Schema dataSchema,
      List<Integer> equalityFieldIds,
      Schema equalityDeleteRowSchema,
      Schema positionDeleteRowSchema) {
    return FlinkFileWriterFactory.builderFor(table)
        .dataSchema(table.schema())
        .dataFileFormat(format())
        .deleteFileFormat(format())
        .equalityFieldIds(ArrayUtil.toIntArray(equalityFieldIds))
        .equalityDeleteRowSchema(equalityDeleteRowSchema)
        .positionDeleteRowSchema(positionDeleteRowSchema)
        .build();
  }

  @Override
  protected RowData toRow(Integer id, String data) {
    return SimpleDataUtil.createRowData(id, data);
  }
}
