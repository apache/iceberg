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

package org.apache.iceberg.spark.data;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.orc.GenericOrcWriters;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;

/**
 * This class acts as an adaptor from an OrcFileAppender to a
 * FileAppender&lt;InternalRow&gt;.
 */
public class SparkOrcWriter implements OrcRowWriter<InternalRow> {

  private final OrcValueWriter<?> writer;

  public SparkOrcWriter(Schema iSchema, TypeDescription orcSchema) {
    Preconditions.checkArgument(orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "Top level must be a struct " + orcSchema);

    writer = OrcSchemaWithTypeVisitor.visit(iSchema, orcSchema, new WriteBuilder());
  }

  @Override
  public void write(InternalRow value, VectorizedRowBatch output) {
    Preconditions.checkArgument(writer instanceof StructWriter, "writer must be StructWriter");
    Preconditions.checkArgument(value != null, "value must not be null");

    int row = output.size;
    output.size += 1;

    ((StructWriter) writer).rootNonNullWrite(row, value, output);
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    return writer.metrics();
  }

  private static class WriteBuilder extends OrcSchemaWithTypeVisitor<OrcValueWriter<?>> {
    private WriteBuilder() {
    }

    @Override
    public OrcValueWriter<?> record(Types.StructType iStruct, TypeDescription record,
                                      List<String> names, List<OrcValueWriter<?>> fields) {
      return new StructWriter(fields, record.getChildren());
    }

    @Override
    public OrcValueWriter<?> list(Types.ListType iList, TypeDescription array,
        OrcValueWriter<?> element) {
      return SparkOrcValueWriters.list(element, array.getChildren());
    }

    @Override
    public OrcValueWriter<?> map(Types.MapType iMap, TypeDescription map,
        OrcValueWriter<?> key, OrcValueWriter<?> value) {
      return SparkOrcValueWriters.map(key, value, map.getChildren());
    }

    @Override
    public OrcValueWriter<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (primitive.getCategory()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case BYTE:
          return GenericOrcWriters.bytes();
        case SHORT:
          return GenericOrcWriters.shorts();
        case DATE:
        case INT:
          return GenericOrcWriters.ints();
        case LONG:
          return GenericOrcWriters.longs();
        case FLOAT:
          return GenericOrcWriters.floats(ORCSchemaUtil.fieldId(primitive));
        case DOUBLE:
          return GenericOrcWriters.doubles(ORCSchemaUtil.fieldId(primitive));
        case BINARY:
          return GenericOrcWriters.byteArrays();
        case STRING:
        case CHAR:
        case VARCHAR:
          return SparkOrcValueWriters.strings();
        case DECIMAL:
          return SparkOrcValueWriters.decimal(primitive.getPrecision(), primitive.getScale());
        case TIMESTAMP_INSTANT:
        case TIMESTAMP:
          return SparkOrcValueWriters.timestampTz();
        default:
          throw new IllegalArgumentException("Unhandled type " + primitive);
      }
    }
  }

  private static class StructWriter implements OrcValueWriter<InternalRow> {
    private final List<OrcValueWriter> writers;
    private final List<FieldGetter<?>> fieldGetters;

    StructWriter(List<OrcValueWriter<?>> writers, List<TypeDescription> orcTypes) {
      this.writers = Lists.newArrayListWithExpectedSize(writers.size());
      this.fieldGetters = Lists.newArrayListWithExpectedSize(orcTypes.size());

      for (int i = 0; i < orcTypes.size(); i++) {
        this.writers.add(writers.get(i));
        fieldGetters.add(createFieldGetter(orcTypes.get(i), orcTypes.size()));
      }
    }

    @Override
    public void nonNullWrite(int rowId, InternalRow value, ColumnVector output) {
      StructColumnVector cv = (StructColumnVector) output;
      write(rowId, value, c -> cv.fields[c]);
    }

    // Special case of writing the root struct
    public void rootNonNullWrite(int rowId, InternalRow value, VectorizedRowBatch output) {
      write(rowId, value, c -> output.cols[c]);
    }

    private void write(int rowId, InternalRow value, Function<Integer, ColumnVector> colVectorAtFunc) {
      for (int c = 0; c < writers.size(); ++c) {
        writers.get(c).write(rowId, fieldGetters.get(c).getFieldOrNull(value, c), colVectorAtFunc.apply(c));
      }
    }

    @Override
    public Class<?> getJavaClass() {
      return InternalRow.class;
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return writers.stream().flatMap(OrcValueWriter::metrics);
    }

  }

  static FieldGetter<?> createFieldGetter(TypeDescription fieldType, int fieldCount) {
    final FieldGetter<?> fieldGetter;
    switch (fieldType.getCategory()) {
      case BOOLEAN:
        fieldGetter = (row, ordinal) -> row.getBoolean(ordinal);
        break;
      case BYTE:
        fieldGetter = (row, ordinal) -> row.getByte(ordinal);
        break;
      case SHORT:
        fieldGetter = (row, ordinal) -> row.getShort(ordinal);
        break;
      case DATE:
      case INT:
        fieldGetter = (row, ordinal) -> row.getInt(ordinal);
        break;
      case LONG:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        fieldGetter = (row, ordinal) -> row.getLong(ordinal);
        break;
      case FLOAT:
        fieldGetter = (row, ordinal) -> row.getFloat(ordinal);
        break;
      case DOUBLE:
        fieldGetter = (row, ordinal) -> row.getDouble(ordinal);
        break;
      case BINARY:
        fieldGetter = (row, ordinal) -> row.getBinary(ordinal);
        // getBinary always makes a copy, so we don't need to worry about it
        // being changed behind our back.
        break;
      case DECIMAL:
        fieldGetter = (row, ordinal) ->
            row.getDecimal(ordinal, fieldType.getPrecision(), fieldType.getScale());
        break;
      case STRING:
      case CHAR:
      case VARCHAR:
        fieldGetter = (row, ordinal) -> row.getUTF8String(ordinal);
        break;
      case STRUCT:
        fieldGetter = (row, ordinal) -> row.getStruct(ordinal, fieldCount);
        break;
      case LIST:
        fieldGetter = (row, ordinal) -> row.getArray(ordinal);
        break;
      case MAP:
        fieldGetter = (row, ordinal) -> row.getMap(ordinal);
        break;
      default:
        throw new IllegalArgumentException();
    }

    return (row, ordinal) -> {
      if (row.isNullAt(ordinal)) {
        return null;
      }
      return fieldGetter.getFieldOrNull(row, ordinal);
    };
  }

  interface FieldGetter<T> extends Serializable {

    /**
     * Returns a value from a complex Spark data holder such ArrayData, InternalRow, etc...
     * Calls the appropriate getter for the expected data type.
     * @param row Spark's data representation
     * @param ordinal index in the data structure (e.g. column index for InterRow, list index in ArrayData, etc..)
     * @return field value at ordinal
     */
    @Nullable
    T getFieldOrNull(SpecializedGetters row, int ordinal);
  }
}
