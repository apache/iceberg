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
package org.apache.iceberg.data.orc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.OrcRowReader;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericOrcReader implements OrcRowReader<Record> {
  private final OrcValueReader<?> reader;

  public GenericOrcReader(
      Schema expectedSchema, TypeDescription readOrcSchema, Map<Integer, ?> idToConstant) {
    this.reader =
        OrcSchemaWithTypeVisitor.visit(
            expectedSchema, readOrcSchema, new ReadBuilder(idToConstant));
  }

  public static OrcRowReader<Record> buildReader(
      Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericOrcReader(expectedSchema, fileSchema, Collections.emptyMap());
  }

  public static OrcRowReader<Record> buildReader(
      Schema expectedSchema, TypeDescription fileSchema, Map<Integer, ?> idToConstant) {
    return new GenericOrcReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  public Record read(VectorizedRowBatch batch, int row) {
    return (Record) reader.read(new StructColumnVector(batch.size, batch.cols), row);
  }

  @Override
  public void setBatchContext(long batchOffsetInFile) {
    reader.setBatchContext(batchOffsetInFile);
  }

  private static class ReadBuilder extends OrcSchemaWithTypeVisitor<OrcValueReader<?>> {
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public OrcValueReader<?> record(
        Types.StructType expected,
        TypeDescription record,
        List<String> names,
        List<OrcValueReader<?>> fields) {
      return GenericOrcReaders.struct(fields, expected, idToConstant);
    }

    @Override
    public OrcValueReader<?> list(
        Types.ListType iList, TypeDescription array, OrcValueReader<?> elementReader) {
      return GenericOrcReaders.array(elementReader);
    }

    @Override
    public OrcValueReader<?> map(
        Types.MapType iMap,
        TypeDescription map,
        OrcValueReader<?> keyReader,
        OrcValueReader<?> valueReader) {
      return GenericOrcReaders.map(keyReader, valueReader);
    }

    @Override
    public OrcValueReader<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (primitive.getCategory()) {
        case BOOLEAN:
          return OrcValueReaders.booleans();
        case BYTE:
        // Iceberg does not have a byte type. Use int
        case SHORT:
        // Iceberg does not have a short type. Use int
        case INT:
          return OrcValueReaders.ints();
        case LONG:
          switch (iPrimitive.typeId()) {
            case TIME:
              return GenericOrcReaders.times();
            case LONG:
              return OrcValueReaders.longs();
            default:
              throw new IllegalStateException(
                  String.format(
                      "Invalid iceberg type %s corresponding to ORC type %s",
                      iPrimitive, primitive));
          }

        case FLOAT:
          return OrcValueReaders.floats();
        case DOUBLE:
          return OrcValueReaders.doubles();
        case DATE:
          return GenericOrcReaders.dates();
        case TIMESTAMP:
          return GenericOrcReaders.timestamps();
        case TIMESTAMP_INSTANT:
          return GenericOrcReaders.timestampTzs();
        case DECIMAL:
          return GenericOrcReaders.decimals();
        case CHAR:
        case VARCHAR:
        case STRING:
          return GenericOrcReaders.strings();
        case BINARY:
          switch (iPrimitive.typeId()) {
            case UUID:
              return GenericOrcReaders.uuids();
            case FIXED:
              return OrcValueReaders.bytes();
            case BINARY:
              return GenericOrcReaders.bytes();
            default:
              throw new IllegalStateException(
                  String.format(
                      "Invalid iceberg type %s corresponding to ORC type %s",
                      iPrimitive, primitive));
          }
        default:
          throw new IllegalArgumentException("Unhandled type " + primitive);
      }
    }
  }
}
