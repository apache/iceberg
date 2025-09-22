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
import java.nio.ByteBuffer;
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
    setDelegate(
        new ConstantColumnReader(sparkType(), descriptor(), convertToSparkValue(value), false));
  }

  @Override
  public void setBatchSize(int batchSize) {
    super.setBatchSize(batchSize);
    delegate().setBatchSize(batchSize);
    setInitialized(true);
  }

  private Object convertToSparkValue(T value) {
    DataType dataType = sparkType();
    // Match the value to Spark internal type if necessary
    if (dataType == DataTypes.StringType && value instanceof String) {
      // the internal type for StringType is UTF8String
      return UTF8String.fromString((String) value);
    } else if (dataType instanceof DecimalType && value instanceof BigDecimal) {
      // the internal type for DecimalType is Decimal
      return Decimal.apply((BigDecimal) value);
    } else if (dataType == DataTypes.BinaryType && value instanceof ByteBuffer) {
      // the internal type for DecimalType is byte[]
      // Iceberg default value should always use HeapBufferBuffer, so calling ByteBuffer.array()
      // should be safe.
      return ((ByteBuffer) value).array();
    } else {
      return value;
    }
  }
}
