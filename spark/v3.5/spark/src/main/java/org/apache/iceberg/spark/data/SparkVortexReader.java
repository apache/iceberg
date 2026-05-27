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

import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.vortex.GenericVortexReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.iceberg.vortex.VortexSchemaWithTypeVisitor;
import org.apache.iceberg.vortex.VortexValueReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/** Read Vortex as Spark {@link InternalRow}. */
public class SparkVortexReader implements VortexRowReader<InternalRow> {

  private final List<VortexValueReader<?>> fieldReaders;

  public SparkVortexReader(
      Schema readSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      Map<Integer, ?> idToConstant) {
    List<Field> fields = fileArrowSchema.getFields();
    List<Types.NestedField> expected = readSchema.columns();
    this.fieldReaders = Lists.newArrayListWithExpectedSize(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      Type icebergType = expected.get(i).type();
      Field arrowField = fields.get(i);
      this.fieldReaders.add(
          VortexSchemaWithTypeVisitor.visit(icebergType, arrowField, SparkReadBuilder.INSTANCE));
    }
  }

  @Override
  public InternalRow read(VectorSchemaRoot batch, int row) {
    GenericInternalRow result = new GenericInternalRow(fieldReaders.size());
    for (int i = 0; i < fieldReaders.size(); i++) {
      VortexValueReader<?> reader = fieldReaders.get(i);
      result.update(i, reader.read(batch.getVector(i), row));
    }
    return result;
  }

  static class SparkReadBuilder extends VortexSchemaWithTypeVisitor<VortexValueReader<?>> {

    static final SparkReadBuilder INSTANCE = new SparkReadBuilder();

    private SparkReadBuilder() {}

    @Override
    public VortexValueReader<?> struct(
        Types.StructType schema, List<Field> fields, List<VortexValueReader<?>> children) {
      return new StructReader(children);
    }

    @Override
    public VortexValueReader<?> list(
        Types.ListType iList, Field listField, VortexValueReader<?> element) {
      throw new UnsupportedOperationException("Vortex LIST types are not supported yet");
    }

    @Override
    public VortexValueReader<?> primitive(Type.PrimitiveType icebergType, Field primField) {
      return switch (icebergType.typeId()) {
        case BOOLEAN -> GenericVortexReaders.bools();
        case INTEGER -> GenericVortexReaders.ints();
        case LONG -> GenericVortexReaders.longs();
        case FLOAT -> GenericVortexReaders.floats();
        case DOUBLE -> GenericVortexReaders.doubles();
        case STRING -> SparkVortexValueReaders.utf8String();
        case BINARY -> GenericVortexReaders.bytes();
        case DECIMAL -> GenericVortexReaders.decimals();
        case TIMESTAMP, TIMESTAMP_NANO -> {
          ArrowType.Timestamp ts = (ArrowType.Timestamp) primField.getType();
          yield SparkVortexValueReaders.timestamp(ts.getUnit());
        }
        case TIME -> {
          ArrowType.Time time = (ArrowType.Time) primField.getType();
          yield SparkVortexValueReaders.time(time.getUnit());
        }
        case DATE -> SparkVortexValueReaders.date();
        case UUID -> SparkVortexValueReaders.uuid();
        default -> throw new UnsupportedOperationException("Unsupported type: " + icebergType);
      };
    }
  }

  static class StructReader implements VortexValueReader<InternalRow> {

    private final List<VortexValueReader<?>> fields;

    private StructReader(List<VortexValueReader<?>> fields) {
      this.fields = fields;
    }

    @Override
    public InternalRow readNonNull(FieldVector vector, int row) {
      org.apache.arrow.vector.complex.StructVector struct =
          (org.apache.arrow.vector.complex.StructVector) vector;
      GenericInternalRow result = new GenericInternalRow(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        VortexValueReader<?> fieldReader = fields.get(i);
        FieldVector child = (FieldVector) struct.getChildByOrdinal(i);
        result.update(i, fieldReader.read(child, row));
      }
      return result;
    }
  }
}
