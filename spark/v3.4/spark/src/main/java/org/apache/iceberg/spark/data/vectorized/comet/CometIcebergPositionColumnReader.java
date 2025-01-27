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
package org.apache.iceberg.spark.data.vectorized.comet;

import org.apache.comet.parquet.MetadataColumnReader;
import org.apache.comet.parquet.Native;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.spark.sql.types.DataTypes;

public class CometIcebergPositionColumnReader extends CometIcebergColumnReader {
  public CometIcebergPositionColumnReader(Types.NestedField field) {
    super(field);
    delegate = new PositionColumnReader(getDescriptor());
  }

  @Override
  public void setBatchSize(int batchSize) {
    delegate.setBatchSize(batchSize);
    this.batchSize = batchSize;
    initialized = true;
  }

  private static class PositionColumnReader extends MetadataColumnReader {
    /** The current position value of the column that are used to initialize this column reader. */
    private long position;

    PositionColumnReader(ColumnDescriptor descriptor) {
      this(descriptor, 0L);
    }

    PositionColumnReader(ColumnDescriptor descriptor, long position) {
      super(DataTypes.LongType, descriptor, false);
      this.position = position;
    }

    @Override
    public void readBatch(int total) {
      Native.resetBatch(nativeHandle);
      Native.setPosition(nativeHandle, position, total);
      position += total;

      super.readBatch(total);
    }
  }
}
