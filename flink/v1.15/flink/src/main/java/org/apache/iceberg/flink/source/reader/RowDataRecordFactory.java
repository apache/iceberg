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
package org.apache.iceberg.flink.source.reader;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.flink.data.RowDataUtil;

class RowDataRecordFactory implements RecordFactory<RowData> {
  private final RowType rowType;
  private final TypeSerializer[] fieldSerializers;

  RowDataRecordFactory(RowType rowType) {
    this.rowType = rowType;
    this.fieldSerializers = createFieldSerializers(rowType);
  }

  static TypeSerializer[] createFieldSerializers(RowType rowType) {
    return rowType.getChildren().stream()
        .map(InternalSerializers::create)
        .toArray(TypeSerializer[]::new);
  }

  @Override
  public RowData[] createBatch(int batchSize) {
    RowData[] arr = new RowData[batchSize];
    for (int i = 0; i < batchSize; ++i) {
      arr[i] = new GenericRowData(rowType.getFieldCount());
    }
    return arr;
  }

  @Override
  public void clone(RowData from, RowData[] batch, int position) {
    // Set the return value from RowDataUtil.clone back to the array.
    // Clone method returns same clone target object (reused) if it is a GenericRowData.
    // Clone method will allocate a new GenericRowData object
    // if the target object is NOT a GenericRowData.
    // So we should always set the clone return value back to the array.
    batch[position] = RowDataUtil.clone(from, batch[position], rowType, fieldSerializers);
  }
}
