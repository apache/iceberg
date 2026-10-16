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

import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class TestVectorizedStructReader {

  @Test
  void structAssemblesChildHoldersInOrder() {
    NestedField lat = NestedField.required(2, "lat", IntegerType.get());
    NestedField lng = NestedField.optional(3, "lng", IntegerType.get());
    NestedField structField = NestedField.required(1, "s", StructType.of(lat, lng));

    VectorizedArrowReader latReader = new VectorizedArrowReader.ConstantVectorReader<>(lat, 7);
    VectorizedArrowReader lngReader = new VectorizedArrowReader.ConstantVectorReader<>(lng, 9);
    VectorizedArrowReader.VectorizedStructReader reader =
        new VectorizedArrowReader.VectorizedStructReader(
            structField, ImmutableList.<VectorizedReader<?>>of(latReader, lngReader), 0, null);

    reader.setBatchSize(128);
    VectorHolder.StructVectorHolder holder = (VectorHolder.StructVectorHolder) reader.read(null, 3);

    assertThat(holder.numValues()).isEqualTo(3);
    assertThat(holder.childHolders()).hasSize(2);
    assertThat(((VectorHolder.ConstantVectorHolder<?>) holder.childHolders().get(0)).getConstant())
        .isEqualTo(7);
    assertThat(((VectorHolder.ConstantVectorHolder<?>) holder.childHolders().get(1)).getConstant())
        .isEqualTo(9);
  }

  @Test
  void variantChildFileBackedLeafResolvesThroughMetadataReader() {
    NestedField metadataField = NestedField.optional(3, "metadata", BinaryType.get());
    NestedField variantField = NestedField.optional(2, "v", VariantType.get());
    ColumnDescriptor metadataColumn =
        new ColumnDescriptor(
            new String[] {"metadata"},
            Types.optional(PrimitiveTypeName.BINARY).named("metadata"),
            0,
            1);
    VectorizedArrowReader metadataLeaf =
        new VectorizedArrowReader(
            metadataColumn, metadataField, ArrowAllocation.rootAllocator(), false);

    VectorizedArrowReader.VectorizedVariantReader fileBacked =
        new VectorizedArrowReader.VectorizedVariantReader(
            variantField, metadataLeaf, VectorizedArrowReader.nulls());
    assertThat(fileBacked.fileBackedLeaf()).isSameAs(metadataLeaf);

    VectorizedArrowReader.VectorizedVariantReader noLeaf =
        new VectorizedArrowReader.VectorizedVariantReader(
            variantField, VectorizedArrowReader.nulls(), VectorizedArrowReader.nulls());
    assertThat(noLeaf.fileBackedLeaf()).isNull();
  }

  @Test
  void nestedStructReusesDescendantPresenceReader() {
    NestedField added = NestedField.optional(4, "added", IntegerType.get());
    NestedField inner = NestedField.optional(2, "inner", StructType.of(added));
    NestedField outer = NestedField.optional(1, "outer", StructType.of(inner));
    ColumnDescriptor presenceColumn =
        new ColumnDescriptor(
            new String[] {"outer", "inner", "innerId"},
            Types.optional(PrimitiveTypeName.INT64).named("innerId"),
            0,
            2);
    VectorizedArrowReader presenceLeaf =
        new VectorizedArrowReader(
            presenceColumn,
            NestedField.optional(3, "innerId", LongType.get()),
            ArrowAllocation.rootAllocator(),
            false);
    VectorizedArrowReader constantChild =
        new VectorizedArrowReader.ConstantVectorReader<>(added, 7);
    VectorizedArrowReader.VectorizedStructReader innerReader =
        new VectorizedArrowReader.VectorizedStructReader(
            inner, ImmutableList.<VectorizedReader<?>>of(constantChild), 2, presenceLeaf);
    VectorizedArrowReader.VectorizedStructReader outerReader =
        new VectorizedArrowReader.VectorizedStructReader(
            outer, ImmutableList.<VectorizedReader<?>>of(innerReader), 1, null);

    // outer has no file-backed child, so it reuses inner's presence reader rather than build one
    assertThat(outerReader.fileBackedLeaf()).isSameAs(presenceLeaf);
  }
}
