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

import java.math.BigDecimal;
import org.apache.comet.parquet.ConstantColumnReader;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.unsafe.types.UTF8String;

class CometConstantColumnReader<T> extends CometColumnReader {

  CometConstantColumnReader(T value, Types.NestedField field) {
    super(field);
    // use delegate to set constant value on the native side to be consumed by native execution.
    delegate =
        new ConstantColumnReader(
            getSparkType(), getDescriptor(), convertToSparkValue(value), false);
  }

  @Override
  public void setBatchSize(int batchSize) {
    delegate.setBatchSize(batchSize);
    this.batchSize = batchSize;
    initialized = true;
  }

  private Object convertToSparkValue(T value) {
    DataType dataType = getSparkType();
    if (dataType == DataTypes.StringType) {
      return UTF8String.fromString((String) value);
    } else if (dataType instanceof DecimalType) {
      return Decimal.apply((BigDecimal) value);
    } else if (dataType == DataTypes.BinaryType) {
      // Iceberg default value should always use HeapBufferBuffer, so calling ByteBuffer.array()
      // should be safe.
      return ((java.nio.ByteBuffer) value).array();
    } else {
      return value;
    }
  }
}
