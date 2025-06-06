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

import dev.vortex.api.Array;
import dev.vortex.api.DType;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.vortex.GenericVortexReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.iceberg.vortex.VortexSchemaWithTypeVisitor;
import org.apache.iceberg.vortex.VortexValueReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/** Read Vortex as Spark {@link InternalRow}. */
public class SparkVortexReader implements VortexRowReader<InternalRow> {
  private final VortexValueReader<?> reader;

  public SparkVortexReader(Schema readSchema, DType vortexSchema, Map<Integer, ?> idToConstant) {
    this.reader =
        VortexSchemaWithTypeVisitor.visit(readSchema, vortexSchema, SparkReadBuilder.INSTANCE);
  }

  @Override
  public InternalRow read(Array batch, int row) {
    return (InternalRow) reader.read(batch, row);
  }

  static class SparkReadBuilder extends VortexSchemaWithTypeVisitor<VortexValueReader<?>> {
    static final SparkReadBuilder INSTANCE = new SparkReadBuilder();

    private SparkReadBuilder() {}

    @Override
    public VortexValueReader<?> struct(
        Types.StructType schema,
        List<DType> types,
        List<String> names,
        List<VortexValueReader<?>> fields) {
      return new StructReader(fields);
    }

    @Override
    public VortexValueReader<?> list(
        Types.ListType iList, DType array, VortexValueReader<?> element) {
      throw new UnsupportedOperationException("Vortex LIST types are not supported yet");
    }

    @Override
    public VortexValueReader<?> primitive(Type.PrimitiveType icebergType, DType vortexType) {
      switch (icebergType.typeId()) {
        case BOOLEAN:
          return GenericVortexReaders.bools();
        case INTEGER:
          return GenericVortexReaders.ints();
        case LONG:
          return GenericVortexReaders.longs();
        case FLOAT:
          return GenericVortexReaders.floats();
        case DOUBLE:
          return GenericVortexReaders.doubles();
        case STRING:
          return SparkVortexValueReaders.utf8String();
        case BINARY:
          // Spark expects binary to be in another format, no?
          return GenericVortexReaders.bytes();
        case DECIMAL:
          return GenericVortexReaders.decimals();
        case TIMESTAMP:
        case TIMESTAMP_NANO:
          // TODO(aduffy): timestamp and date types
          return SparkVortexValueReaders.timestamp(vortexType.getTimeUnit());
        case DATE:
          // TODO(aduffy): timestamp and date types
          return SparkVortexValueReaders.date(vortexType.getTimeUnit());
        case TIME:
          // TODO(aduffy): timestamp and date types
        default:
          throw new UnsupportedOperationException("Unsupported type: " + icebergType);
      }
    }
  }

  static class StructReader implements VortexValueReader<InternalRow> {
    private final List<VortexValueReader<?>> fields;

    private StructReader(List<VortexValueReader<?>> fields) {
      this.fields = fields;
    }

    @Override
    public InternalRow readNonNull(Array array, int row) {
      GenericInternalRow result = new GenericInternalRow(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        VortexValueReader<?> fieldReader = fields.get(i);
        Object field = fieldReader.read(array.getField(i), row);
        result.update(i, field);
      }
      return result;
    }
  }
}
