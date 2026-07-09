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

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class TestVectorizedTimestampMillisDictionary {

  private BufferAllocator allocator;

  @BeforeEach
  public void before() {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void after() {
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testTimestampMillisDictionaryProxyScaling() throws Exception {
    // 1. Mock Parquet descriptor structures to avoid package-private constructor limitations
    ColumnDescriptor mockDescriptor = Mockito.mock(ColumnDescriptor.class);
    PrimitiveType mockPrimitiveType = Mockito.mock(PrimitiveType.class);
    LogicalTypeAnnotation.TimestampLogicalTypeAnnotation mockAnnotation =
        Mockito.mock(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class);

    Mockito.when(mockDescriptor.getPrimitiveType()).thenReturn(mockPrimitiveType);
    Mockito.when(mockPrimitiveType.getLogicalTypeAnnotation()).thenReturn(mockAnnotation);
    Mockito.when(mockAnnotation.getUnit()).thenReturn(LogicalTypeAnnotation.TimeUnit.MILLIS);
    Mockito.when(mockDescriptor.getPath())
        .thenReturn(new String[] {"reserved_incorta_extraction_timestamp"});

    Types.NestedField icebergField =
        Types.NestedField.optional(
            1, "reserved_incorta_extraction_timestamp", Types.TimestampType.withZone());

    PageReadStore mockPageReadStore = Mockito.mock(PageReadStore.class);
    ColumnChunkMetaData mockChunkMeta = Mockito.mock(ColumnChunkMetaData.class);
    Dictionary mockParquetDictionary = Mockito.mock(Dictionary.class);

    ColumnPath columnPath = ColumnPath.get("reserved_incorta_extraction_timestamp");
    Map<ColumnPath, ColumnChunkMetaData> metadataMap = Maps.newHashMap();
    metadataMap.put(columnPath, mockChunkMeta);

    Mockito.when(mockParquetDictionary.getEncoding()).thenReturn(Encoding.PLAIN_DICTIONARY);
    Mockito.when(mockParquetDictionary.decodeToLong(0)).thenReturn(1783426418473L);

    // 2. Intercept internal instantiation of VectorizedColumnIterator using construction mocking
    try (MockedConstruction<VectorizedColumnIterator> mockedConstruction =
        Mockito.mockConstruction(
            VectorizedColumnIterator.class,
            (mockIterator, context) -> {
              Mockito.when(mockIterator.setRowGroupInfo(Mockito.any(), Mockito.anyBoolean()))
                  .thenReturn(mockParquetDictionary);
            })) {

      VectorizedArrowReader reader =
          new VectorizedArrowReader(mockDescriptor, icebergField, allocator, true);

      // 3. Execute target method
      reader.setRowGroupInfo(mockPageReadStore, metadataMap);

      // 4. Extract private field using Java Reflection to bypass missing getters
      Field dictionaryField = VectorizedArrowReader.class.getDeclaredField("dictionary");
      dictionaryField.setAccessible(true);
      Dictionary activeDictionary = (Dictionary) dictionaryField.get(reader);

      // 5. Run Verification Assertions
      assertThat(activeDictionary)
          .as("The internal dictionary reference must be substituted by our proxy class")
          .isNotNull()
          .isNotEqualTo(mockParquetDictionary);

      long expectedMicros = 1783426418473L * 1000L;
      assertThat(activeDictionary.decodeToLong(0))
          .as(
              "The proxy hook must intercept lookups and multiply millisecond properties to microseconds")
          .isEqualTo(expectedMicros);
    }
  }
}
