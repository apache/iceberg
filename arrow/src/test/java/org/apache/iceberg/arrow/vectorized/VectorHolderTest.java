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

import static org.mockito.Mockito.when;

import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class VectorHolderTest {
  @Mock ColumnDescriptor columnDescriptor;
  @Mock FieldVector vector;
  @Mock Dictionary dictionary;
  @Mock NullabilityHolder nullabilityHolder;
  @Mock Types.NestedField icebergField;

  VectorHolder vectorHolder;

  @BeforeEach
  void before() {
    MockitoAnnotations.initMocks(this);
    vectorHolder =
        new VectorHolder(
            columnDescriptor, vector, false, dictionary, nullabilityHolder, icebergField);
  }

  @Test
  void testDescriptor() {
    ColumnDescriptor result = vectorHolder.descriptor();
    Assertions.assertSame(this.columnDescriptor, result);
  }

  @Test
  void testVector() {
    FieldVector result = vectorHolder.vector();
    Assertions.assertSame(this.vector, result);
  }

  @Test
  void testDictionary() {
    Dictionary result = vectorHolder.dictionary();
    Assertions.assertSame(this.dictionary, result);
  }

  @Test
  void testNullabilityHolder() {
    NullabilityHolder result = vectorHolder.nullabilityHolder();
    Assertions.assertSame(this.nullabilityHolder, result);
  }

  @Test
  void testIcebergType() {
    when(icebergField.type()).thenReturn(Types.LongType.get());

    Type result = vectorHolder.icebergType();
    Assertions.assertEquals(Types.LongType.get(), result);
  }

  @Test
  void testIcebergField() {
    Types.NestedField result = vectorHolder.icebergField();
    Assertions.assertSame(this.icebergField, result);
  }

  @Test
  void testNumValues() {
    when(vector.getValueCount()).thenReturn(88);

    int result = vectorHolder.numValues();
    Assertions.assertEquals(88, result);
  }

  @Test
  void testDummyHolder() {
    VectorHolder result = VectorHolder.dummyHolder(88);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(88, result.numValues());
  }

  @Test
  void testIsDummy1() {
    VectorHolder vh = VectorHolder.dummyHolder(0);
    boolean result = vh.isDummy();
    Assertions.assertEquals(true, result);
  }

  @Test
  void testIsDummy2() {
    VectorHolder vh = VectorHolder.constantHolder(this.icebergField, 0, "a");
    boolean result = vh.isDummy();
    Assertions.assertEquals(true, result);
  }
}
