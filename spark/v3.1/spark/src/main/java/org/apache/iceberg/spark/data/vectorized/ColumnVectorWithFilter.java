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
package org.apache.iceberg.spark.data.vectorized;

import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.arrow.vectorized.VectorHolder.ConstantVectorHolder;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.types.UTF8String;

public class ColumnVectorWithFilter extends IcebergArrowColumnVector {
  private final int[] rowIdMapping;

  public ColumnVectorWithFilter(VectorHolder holder, int[] rowIdMapping) {
    super(holder);
    this.rowIdMapping = rowIdMapping;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullabilityHolder().isNullAt(rowIdMapping[rowId]) == 1;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor().getBoolean(rowIdMapping[rowId]);
  }

  @Override
  public int getInt(int rowId) {
    return accessor().getInt(rowIdMapping[rowId]);
  }

  @Override
  public long getLong(int rowId) {
    return accessor().getLong(rowIdMapping[rowId]);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor().getFloat(rowIdMapping[rowId]);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor().getDouble(rowIdMapping[rowId]);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor().getArray(rowIdMapping[rowId]);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor().getDecimal(rowIdMapping[rowId], precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor().getUTF8String(rowIdMapping[rowId]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor().getBinary(rowIdMapping[rowId]);
  }

  public static ColumnVector forHolder(VectorHolder holder, int[] rowIdMapping, int numRows) {
    return holder.isDummy()
        ? new ConstantColumnVector(
            Types.IntegerType.get(), numRows, ((ConstantVectorHolder) holder).getConstant())
        : new ColumnVectorWithFilter(holder, rowIdMapping);
  }
}
