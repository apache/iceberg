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
package org.apache.iceberg.arrow.vectorized;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.arrow.vectorized.GenericArrowVectorAccessorFactory.DecimalFactory;
import org.apache.iceberg.arrow.vectorized.GenericArrowVectorAccessorFactory.StringFactory;

final class ArrowVectorAccessors {

  private static final GenericArrowVectorAccessorFactory<?, String, ?, ?> FACTORY;

  static {
    FACTORY =
        new GenericArrowVectorAccessorFactory<>(
            JavaDecimalFactory::new,
            JavaStringFactory::new,
            throwingSupplier("Struct type is not supported"),
            throwingSupplier("List type is not supported"));
  }

  private static <T> Supplier<T> throwingSupplier(String message) {
    return () -> {
      throw new UnsupportedOperationException(message);
    };
  }

  private ArrowVectorAccessors() {
    throw new UnsupportedOperationException(
        ArrowVectorAccessors.class.getName() + " cannot be instantiated.");
  }

  static ArrowVectorAccessor<?, String, ?, ?> getVectorAccessor(VectorHolder holder) {
    return FACTORY.getVectorAccessor(holder);
  }

  private static final class JavaStringFactory implements StringFactory<String> {
    @Override
    public Class<String> getGenericClass() {
      return String.class;
    }

    @Override
    public String ofRow(VarCharVector vector, int rowId) {
      return ofBytes(vector.get(rowId));
    }

    @Override
    public String ofRow(BigIntVector vector, int rowId) {
      return String.valueOf(vector.get(rowId));
    }

    @Override
    public String ofBytes(byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public String ofByteBuffer(ByteBuffer byteBuffer) {
      if (byteBuffer.hasArray()) {
        return new String(
            byteBuffer.array(),
            byteBuffer.arrayOffset() + byteBuffer.position(),
            byteBuffer.remaining(),
            StandardCharsets.UTF_8);
      }
      byte[] bytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static final class JavaDecimalFactory implements DecimalFactory<BigDecimal> {

    @Override
    public Class<BigDecimal> getGenericClass() {
      return BigDecimal.class;
    }

    @Override
    public BigDecimal ofLong(long value, int precision, int scale) {
      return BigDecimal.valueOf(value, scale);
    }

    @Override
    public BigDecimal ofBigDecimal(BigDecimal value, int precision, int scale) {
      return BigDecimal.valueOf(value.unscaledValue().longValue(), scale);
    }
  }
}
