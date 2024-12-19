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

import org.apache.comet.vector.CometDelegateVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

@SuppressWarnings("checkstyle:VisibilityModifier")
class CometVector extends CometDelegateVector {

  // the rowId mapping to skip deleted rows for all column vectors inside a batch
  // Here is an example:
  // [0,1,2,3,4,5,6,7] -- Original status of the row id mapping array
  // Position delete 2, 6
  // [0,1,3,4,5,7,-,-] -- After applying position deletes [Set Num records to 6]
  // Equality delete 1 <= x <= 3
  // [0,4,5,7,-,-,-,-] -- After applying equality deletes [Set Num records to 4]
  protected int[] rowIdMapping;

  CometVector(DataType type, boolean useDecimal128) {
    super(type, useDecimal128);
  }

  public void setRowIdMapping(int[] rowIdMapping) {
    this.rowIdMapping = rowIdMapping;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return super.isNullAt(mapRowId(rowId));
  }

  @Override
  public boolean getBoolean(int rowId) {
    return super.getBoolean(mapRowId(rowId));
  }

  @Override
  public byte getByte(int rowId) {
    return super.getByte(mapRowId(rowId));
  }

  @Override
  public short getShort(int rowId) {
    return super.getShort(mapRowId(rowId));
  }

  @Override
  public int getInt(int rowId) {
    return super.getInt(mapRowId(rowId));
  }

  @Override
  public long getLong(int rowId) {
    return super.getLong(mapRowId(rowId));
  }

  @Override
  public float getFloat(int rowId) {
    return super.getFloat(mapRowId(rowId));
  }

  @Override
  public double getDouble(int rowId) {
    return super.getDouble(mapRowId(rowId));
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }

    return super.getDecimal(mapRowId(rowId), precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }

    return super.getUTF8String(mapRowId(rowId));
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }

    return super.getBinary(mapRowId(rowId));
  }

  private int mapRowId(int rowId) {
    if (rowIdMapping != null) {
      return rowIdMapping[rowId];
    }

    return rowId;
  }
}
