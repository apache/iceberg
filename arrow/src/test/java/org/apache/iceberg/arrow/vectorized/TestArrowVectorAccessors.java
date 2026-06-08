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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.RunEndEncodedVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Arrow 19 vector types newly handled by {@link GenericArrowVectorAccessorFactory}:
 * the String/Binary view vectors and the run-end-encoded vector. These encodings are not produced
 * by Iceberg's own readers, so the vectors are constructed directly here and read back through the
 * accessor. (The canonical UUID extension vector is covered end-to-end by {@link TestArrowReader}.)
 */
public class TestArrowVectorAccessors {

  private final TestAccessorFactory factory = new TestAccessorFactory();
  private BufferAllocator allocator;

  @BeforeEach
  public void before() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void after() {
    allocator.close();
  }

  @Test
  public void stringViewAccessor() {
    try (ViewVarCharVector vector = new ViewVarCharVector("s", allocator)) {
      vector.allocateNew();
      vector.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
      // long enough to exercise the out-of-line (non-inlined) view representation
      vector.setSafe(1, "a string that does not fit inline".getBytes(StandardCharsets.UTF_8));
      vector.setValueCount(2);

      ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessor =
          accessorFor(
              vector,
              PrimitiveTypeName.BINARY,
              NestedField.optional(0, "s", Types.StringType.get()));
      assertThat(accessor.getUTF8String(0)).isEqualTo("hello");
      assertThat(accessor.getUTF8String(1)).isEqualTo("a string that does not fit inline");
    }
  }

  @Test
  public void binaryViewAccessor() {
    try (ViewVarBinaryVector vector = new ViewVarBinaryVector("b", allocator)) {
      vector.allocateNew();
      vector.setSafe(0, new byte[] {1, 2, 3});
      vector.setSafe(1, new byte[] {4, 5, 6, 7});
      vector.setValueCount(2);

      ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessor =
          accessorFor(
              vector,
              PrimitiveTypeName.BINARY,
              NestedField.optional(0, "b", Types.BinaryType.get()));
      assertThat(accessor.getBinary(0)).containsExactly(1, 2, 3);
      assertThat(accessor.getBinary(1)).containsExactly(4, 5, 6, 7);
    }
  }

  @Test
  public void runEndEncodedAccessor() {
    // 5 logical rows over 2 runs: value 10 for rows [0, 3), value 20 for rows [3, 5).
    Field runEndsField =
        new Field("run_ends", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field valuesField = new Field("values", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field reeField =
        new Field(
            "ree",
            FieldType.notNullable(ArrowType.RunEndEncoded.INSTANCE),
            Arrays.asList(runEndsField, valuesField));

    IntVector runEnds = (IntVector) runEndsField.createVector(allocator);
    IntVector values = (IntVector) valuesField.createVector(allocator);
    // The run-end-encoded vector owns its children and closes them on close().
    try (RunEndEncodedVector vector =
        new RunEndEncodedVector(reeField, allocator, runEnds, values, null)) {
      runEnds.setSafe(0, 3);
      runEnds.setSafe(1, 5);
      runEnds.setValueCount(2);
      values.setSafe(0, 10);
      values.setSafe(1, 20);
      values.setValueCount(2);
      vector.setValueCount(5);

      ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessor =
          accessorFor(
              vector,
              PrimitiveTypeName.INT32,
              NestedField.optional(0, "v", Types.IntegerType.get()));
      assertThat(accessor.getInt(0)).isEqualTo(10);
      assertThat(accessor.getInt(2)).isEqualTo(10);
      assertThat(accessor.getInt(3)).isEqualTo(20);
      assertThat(accessor.getInt(4)).isEqualTo(20);
    }
  }

  @Test
  public void listViewAccessor() {
    Field dataField = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field listViewField =
        new Field(
            "list", FieldType.nullable(ArrowType.ListView.INSTANCE), Arrays.asList(dataField));
    try (ListViewVector vector = (ListViewVector) listViewField.createVector(allocator)) {
      vector.allocateNew();
      IntVector data = (IntVector) vector.getDataVector();
      data.setSafe(0, 10);
      data.setSafe(1, 20);
      data.setSafe(2, 30);
      data.setSafe(3, 40);
      data.setValueCount(4);
      // row 0 -> [10, 20, 30], row 1 -> [40]
      vector.setValidity(0, 1);
      vector.setOffset(0, 0);
      vector.setSize(0, 3);
      vector.setValidity(1, 1);
      vector.setOffset(1, 3);
      vector.setSize(1, 1);
      vector.setValueCount(2);

      ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessor =
          accessorFor(vector, PrimitiveTypeName.INT32, listField());
      assertThat(accessor.getArray(0)).isEqualTo(Arrays.asList(10, 20, 30));
      assertThat(accessor.getArray(1)).isEqualTo(Arrays.asList(40));
    }
  }

  @Test
  public void largeListViewAccessor() {
    Field dataField = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field largeListViewField =
        new Field(
            "list", FieldType.nullable(ArrowType.LargeListView.INSTANCE), Arrays.asList(dataField));
    try (LargeListViewVector vector =
        (LargeListViewVector) largeListViewField.createVector(allocator)) {
      vector.allocateNew();
      IntVector data = (IntVector) vector.getDataVector();
      data.setSafe(0, 10);
      data.setSafe(1, 20);
      data.setSafe(2, 30);
      data.setSafe(3, 40);
      data.setValueCount(4);
      // row 0 -> [10, 20, 30], row 1 -> [40]
      vector.setValidity(0, 1);
      vector.setOffset(0, 0);
      vector.setSize(0, 3);
      vector.setValidity(1, 1);
      vector.setOffset(1, 3);
      vector.setSize(1, 1);
      vector.setValueCount(2);

      ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessor =
          accessorFor(vector, PrimitiveTypeName.INT32, listField());
      assertThat(accessor.getArray(0)).isEqualTo(Arrays.asList(10, 20, 30));
      assertThat(accessor.getArray(1)).isEqualTo(Arrays.asList(40));
    }
  }

  @Test
  public void largeListAccessor() {
    Field dataField = new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field largeListField =
        new Field(
            "list", FieldType.nullable(ArrowType.LargeList.INSTANCE), Arrays.asList(dataField));
    try (LargeListVector vector = (LargeListVector) largeListField.createVector(allocator)) {
      // row 0 -> [10, 20, 30], row 1 -> [40]
      UnionLargeListWriter writer = vector.getWriter();
      writer.startList();
      writer.writeInt(10);
      writer.writeInt(20);
      writer.writeInt(30);
      writer.endList();
      writer.startList();
      writer.writeInt(40);
      writer.endList();
      writer.setValueCount(2);

      ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessor =
          accessorFor(vector, PrimitiveTypeName.INT32, listField());
      assertThat(accessor.getArray(0)).isEqualTo(Arrays.asList(10, 20, 30));
      assertThat(accessor.getArray(1)).isEqualTo(Arrays.asList(40));
    }
  }

  private static NestedField listField() {
    return NestedField.optional(0, "l", Types.ListType.ofOptional(1, Types.IntegerType.get()));
  }

  private ArrowVectorAccessor<BigDecimal, String, Object, ValueVector> accessorFor(
      FieldVector vector, PrimitiveTypeName typeName, NestedField field) {
    ColumnDescriptor descriptor =
        new ColumnDescriptor(
            new String[] {"col"},
            org.apache.parquet.schema.Types.optional(typeName).named("col"),
            0,
            0);
    VectorHolder holder =
        new VectorHolder(
            descriptor, vector, false, null, new NullabilityHolder(vector.getValueCount()), field);
    return factory.getVectorAccessor(holder);
  }

  /**
   * A concrete factory that materializes decimals as {@link BigDecimal} and strings as Java {@link
   * String}.
   */
  private static class TestAccessorFactory
      extends GenericArrowVectorAccessorFactory<BigDecimal, String, Object, ValueVector> {
    TestAccessorFactory() {
      super(
          JavaDecimalFactory::new,
          JavaStringFactory::new,
          () -> {
            throw new UnsupportedOperationException("struct factory not needed");
          },
          IntListFactory::new);
    }
  }

  /**
   * Materializes a list element range as a {@link List} of the child {@link IntVector}'s values.
   */
  private static class IntListFactory
      implements GenericArrowVectorAccessorFactory.ArrayFactory<ValueVector, Object> {
    @Override
    public ValueVector ofChild(ValueVector childVector) {
      return childVector;
    }

    @Override
    public Object ofRow(ValueVector childData, int elementStart, int numElements) {
      IntVector child = (IntVector) childData;
      List<Integer> values = new ArrayList<>(numElements);
      for (int i = 0; i < numElements; i++) {
        values.add(child.get(elementStart + i));
      }
      return values;
    }
  }

  private static class JavaStringFactory
      implements GenericArrowVectorAccessorFactory.StringFactory<String> {
    @Override
    public Class<String> getGenericClass() {
      return String.class;
    }

    @Override
    public String ofRow(VarCharVector vector, int rowId) {
      return new String(vector.get(rowId), StandardCharsets.UTF_8);
    }

    @Override
    public String ofRow(ViewVarCharVector vector, int rowId) {
      return new String(vector.get(rowId), StandardCharsets.UTF_8);
    }

    @Override
    public String ofBytes(byte[] bytes) {
      return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public String ofByteBuffer(ByteBuffer byteBuffer) {
      return StandardCharsets.UTF_8.decode(byteBuffer).toString();
    }
  }

  private static class JavaDecimalFactory
      implements GenericArrowVectorAccessorFactory.DecimalFactory<BigDecimal> {
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
      return value;
    }
  }
}
