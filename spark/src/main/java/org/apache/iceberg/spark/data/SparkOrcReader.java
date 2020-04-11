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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValReader;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;

/**
 * Converts the OrcInterator, which returns ORC's VectorizedRowBatch to a
 * set of Spark's UnsafeRows.
 *
 * It minimizes allocations by reusing most of the objects in the implementation.
 */
public class SparkOrcReader implements OrcValueReader<InternalRow> {
  private final SparkOrcValueReaders.StructReader reader;

  public SparkOrcReader(org.apache.iceberg.Schema expectedSchema, TypeDescription readSchema) {
    this(expectedSchema, readSchema, ImmutableMap.of());
  }

  @SuppressWarnings("unchecked")
  public SparkOrcReader(
      org.apache.iceberg.Schema expectedSchema, TypeDescription readOrcSchema, Map<Integer, ?> idToConstant) {
    reader = (SparkOrcValueReaders.StructReader) OrcSchemaWithTypeVisitor.visit(
        expectedSchema, readOrcSchema, new ReadBuilder(idToConstant));
  }

  @Override
  public InternalRow read(VectorizedRowBatch batch, int row) {
    return reader.read(batch, row);
  }

  private static class ReadBuilder extends OrcSchemaWithTypeVisitor<OrcValReader<?>> {
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public OrcValReader<?> record(
        Types.StructType expected, TypeDescription record, List<String> names, List<OrcValReader<?>> fields) {
      return SparkOrcValueReaders.struct(fields, expected, idToConstant);
    }

    @Override
    public OrcValReader<?> array(Types.ListType iList, TypeDescription array, OrcValReader<?> elementReader) {
      return SparkOrcValueReaders.array(elementReader);
    }

    @Override
    public OrcValReader<?> map(
        Types.MapType iMap, TypeDescription map, OrcValReader<?> keyReader, OrcValReader<?> valueReader) {
      return SparkOrcValueReaders.map(keyReader, valueReader);
    }

    @Override
    public OrcValReader<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (primitive.getCategory()) {
        case BOOLEAN:
          return OrcValueReaders.booleans();
        case BYTE:
          return OrcValueReaders.byteReader();
        case SHORT:
          return OrcValueReaders.shorts();
        case DATE:
        case INT:
          return OrcValueReaders.ints();
        case LONG:
          return OrcValueReaders.longs();
        case FLOAT:
          return OrcValueReaders.floats();
        case DOUBLE:
          return OrcValueReaders.doubles();
        case TIMESTAMP_INSTANT:
          return SparkOrcValueReaders.timestampTzs();
        case DECIMAL:
          if (primitive.getPrecision() <= Decimal.MAX_LONG_DIGITS()) {
            return new SparkOrcValueReaders.Decimal18Reader(primitive.getPrecision(), primitive.getScale());
          } else {
            return new SparkOrcValueReaders.Decimal38Reader(primitive.getPrecision(), primitive.getScale());
          }
        case CHAR:
        case VARCHAR:
        case STRING:
          return SparkOrcValueReaders.strings();
        case BINARY:
          return OrcValueReaders.bytes();
        default:
          throw new IllegalArgumentException("Unhandled type " + primitive);
      }
    }
  }
}
