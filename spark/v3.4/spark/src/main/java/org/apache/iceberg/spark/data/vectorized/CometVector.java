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
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.isNullAt(newRowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getBoolean(newRowId);
  }

  @Override
  public byte getByte(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getByte(newRowId);
  }

  @Override
  public short getShort(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getShort(newRowId);
  }

  @Override
  public int getInt(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getInt(newRowId);
  }

  @Override
  public long getLong(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getLong(newRowId);
  }

  @Override
  public float getFloat(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getFloat(newRowId);
  }

  @Override
  public double getDouble(int rowId) {
    int newRowId = rowId;
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getDouble(newRowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    int newRowId = rowId;
    if (isNullAt(newRowId)) {
      return null;
    }
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getDecimal(newRowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    int newRowId = rowId;
    if (isNullAt(newRowId)) {
      return null;
    }
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getUTF8String(newRowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    int newRowId = rowId;
    if (isNullAt(newRowId)) {
      return null;
    }
    if (rowIdMapping != null) {
      newRowId = rowIdMapping[rowId];
    }
    return super.getBinary(newRowId);
  }
}
