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

package com.netflix.iceberg.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.io.api.Binary;

public abstract class ColumnWriter<T> implements TripleWriter<T> {
  @SuppressWarnings("unchecked")
  static <T> ColumnWriter<T> newWriter(ColumnDescriptor desc) {
    switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN:
        return (ColumnWriter<T>) new ColumnWriter<Boolean>(desc) {
          @Override
          public void write(int rl, Boolean value) {
            writeBoolean(rl, value);
          }
        };
      case INT32:
        return (ColumnWriter<T>) new ColumnWriter<Integer>(desc) {
          @Override
          public void write(int rl, Integer value) {
            writeInteger(rl, value);
          }
        };
      case INT64:
        return (ColumnWriter<T>) new ColumnWriter<Long>(desc) {
          @Override
          public void write(int rl, Long value) {
            writeLong(rl, value);
          }
        };
      case FLOAT:
        return (ColumnWriter<T>) new ColumnWriter<Float>(desc) {
          @Override
          public void write(int rl, Float value) {
            writeFloat(rl, value);
          }
        };
      case DOUBLE:
        return (ColumnWriter<T>) new ColumnWriter<Double>(desc) {
          @Override
          public void write(int rl, Double value) {
            writeDouble(rl, value);
          }
        };
      case FIXED_LEN_BYTE_ARRAY:
      case BINARY:
        return (ColumnWriter<T>) new ColumnWriter<Binary>(desc) {
          @Override
          public void write(int rl, Binary value) {
            writeBinary(rl, value);
          }
        };
      default:
        throw new UnsupportedOperationException("Unsupported primitive type: "
                + desc.getPrimitiveType().getPrimitiveTypeName());
    }
  }

  private final ColumnDescriptor desc;
  private final int maxDefinitionLevel;

  private long triplesCount = 0L;
  private org.apache.parquet.column.ColumnWriter columnWriter = null;

  private ColumnWriter(ColumnDescriptor desc) {
    this.desc = desc;
    this.maxDefinitionLevel = desc.getMaxDefinitionLevel();
  }

  public void setColumnStore(ColumnWriteStore columnStore) {
    this.columnWriter = columnStore.getColumnWriter(desc);
  }

  @Override
  public void writeBoolean(int rl, boolean value) {
    this.triplesCount += 1;
    columnWriter.write(value, rl, maxDefinitionLevel);
  }

  @Override
  public void writeInteger(int rl, int value) {
    this.triplesCount += 1;
    columnWriter.write(value, rl, maxDefinitionLevel);
  }

  @Override
  public void writeLong(int rl, long value) {
    this.triplesCount += 1;
    columnWriter.write(value, rl, maxDefinitionLevel);
  }

  @Override
  public void writeFloat(int rl, float value) {
    this.triplesCount += 1;
    columnWriter.write(value, rl, maxDefinitionLevel);
  }

  @Override
  public void writeDouble(int rl, double value) {
    this.triplesCount += 1;
    columnWriter.write(value, rl, maxDefinitionLevel);
  }

  @Override
  public void writeBinary(int rl, Binary value) {
    this.triplesCount += 1;
    columnWriter.write(value, rl, maxDefinitionLevel);
  }

  @Override
  public void writeNull(int rl, int dl) {
    this.triplesCount += 1;
    columnWriter.writeNull(rl, dl);
  }
}
