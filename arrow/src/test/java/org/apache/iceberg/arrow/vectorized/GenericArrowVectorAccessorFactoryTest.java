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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.function.Supplier;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class GenericArrowVectorAccessorFactoryTest {
  @Mock
  Supplier<GenericArrowVectorAccessorFactory.DecimalFactory<BigDecimal>> decimalFactorySupplier;

  @Mock Supplier<GenericArrowVectorAccessorFactory.StringFactory<String>> stringFactorySupplier;

  @Mock
  Supplier<
          GenericArrowVectorAccessorFactory.StructChildFactory<
              org.apache.spark.sql.vectorized.ArrowColumnVector>>
      structChildFactorySupplier;

  @Mock
  Supplier<
          GenericArrowVectorAccessorFactory.ArrayFactory<
              org.apache.spark.sql.vectorized.ArrowColumnVector,
              org.apache.spark.sql.vectorized.ColumnarArray>>
      arrayFactorySupplier;

  @InjectMocks GenericArrowVectorAccessorFactory genericArrowVectorAccessorFactory;

  @BeforeEach
  void before() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testGetVectorAccessorWithIntVector() {
    IntVector vector = mock(IntVector.class);
    when(vector.get(0)).thenReturn(88);

    Types.NestedField nestedField = Types.NestedField.optional(0, "a1", Types.IntegerType.get());
    ColumnDescriptor columnDescriptor =
        new ColumnDescriptor(
            new String[] {nestedField.name()}, PrimitiveType.PrimitiveTypeName.INT32, 0, 1);
    NullabilityHolder nullabilityHolder = new NullabilityHolder(10000);
    VectorHolder vectorHolder =
        new VectorHolder(columnDescriptor, vector, false, null, nullabilityHolder, nestedField);
    ArrowVectorAccessor actual = genericArrowVectorAccessorFactory.getVectorAccessor(vectorHolder);
    assertThat(actual).isNotNull();
    assertThat(actual).isInstanceOf(ArrowVectorAccessor.class);
    int intValue = actual.getInt(0);
    assertThat(intValue).isEqualTo(88);
  }

  @Test
  void testGetVectorAccessorWithNullVector() {
    assertThatThrownBy(
            () -> {
              genericArrowVectorAccessorFactory.getVectorAccessor(VectorHolder.dummyHolder(1));
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported vector: null");
  }
}
