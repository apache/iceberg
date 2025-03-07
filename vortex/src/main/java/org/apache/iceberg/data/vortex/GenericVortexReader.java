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
package org.apache.iceberg.data.vortex;

import dev.vortex.api.Array;
import dev.vortex.api.DType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.iceberg.vortex.VortexSchemaWithTypeVisitor;
import org.apache.iceberg.vortex.VortexValueReader;

public class GenericVortexReader implements VortexRowReader<Record> {
  private final VortexValueReader<?> reader;

  private GenericVortexReader(
      Schema expectedSchema, DType readVortexSchema, Map<Integer, ?> idToConstant) {
    this.reader =
        VortexSchemaWithTypeVisitor.visit(
            expectedSchema, readVortexSchema, new GenericReadBuilder(idToConstant));
  }

  public static VortexRowReader<Record> buildReader(Schema expectedSchema, DType fileSchema) {
    return new GenericVortexReader(expectedSchema, fileSchema, Collections.emptyMap());
  }

  public static VortexRowReader<Record> buildReader(
      Schema expectedSchema, DType fileSchema, Map<Integer, ?> idToConstant) {
    return new GenericVortexReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  public Record read(Array batch, int row) {
    return (Record) this.reader.read(batch, row);
  }

  @SuppressWarnings("UnusedVariable")
  static class GenericReadBuilder extends VortexSchemaWithTypeVisitor<VortexValueReader<?>> {
    // TODO(aduffy): implement constant readers to fill in identity partition values
    private final Map<Integer, ?> idToConstant;

    private GenericReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public VortexValueReader<?> struct(
        Types.StructType iStruct,
        List<DType> types,
        List<String> names,
        List<VortexValueReader<?>> fields) {
      return GenericVortexReaders.struct(iStruct, fields);
    }

    @Override
    public VortexValueReader<?> list(
        Types.ListType iList, DType array, VortexValueReader<?> element) {
      // TODO(aduffy): implement list reader
      throw new UnsupportedOperationException("LIST TYPES!");
    }

    @Override
    public VortexValueReader<?> primitive(Type.PrimitiveType iPrimitive, DType vortexType) {
      switch (vortexType.getVariant()) {
        case NULL:
          throw new UnsupportedOperationException("Vortex Null type not supported");
        case BOOL:
          return GenericVortexReaders.bools();
        case PRIMITIVE_U8:
        case PRIMITIVE_I8:
        case PRIMITIVE_U16:
        case PRIMITIVE_I16:
        case PRIMITIVE_U32:
        case PRIMITIVE_I32:
          // Types that promote to Iceberg INTEGER
          return GenericVortexReaders.ints();
        case PRIMITIVE_U64:
        case PRIMITIVE_I64:
          // Types that promote to Iceberg LONG
          return GenericVortexReaders.longs();
        case PRIMITIVE_F16:
          throw new UnsupportedOperationException("Vortex F16 type not supported");
        case PRIMITIVE_F32:
          return GenericVortexReaders.floats();
        case PRIMITIVE_F64:
          return GenericVortexReaders.doubles();
        case UTF8:
          return GenericVortexReaders.strings();
        case BINARY:
          return GenericVortexReaders.bytes();
        case EXTENSION:
          // TODO(aduffy): implement TIME/DATE/TIMESTAMP support
          if (vortexType.isDate()) {
            boolean isMillis = vortexType.getTimeUnit() == DType.TimeUnit.MILLISECONDS;
            return GenericVortexReaders.date(isMillis);
          } else if (vortexType.isTimestamp()) {
            Optional<String> timeZone = vortexType.getTimeZone();
            boolean isNanosecond = vortexType.getTimeUnit() == DType.TimeUnit.NANOSECONDS;

            if (timeZone.isEmpty()) {
              return GenericVortexReaders.timestamp(isNanosecond);
            } else {
              return GenericVortexReaders.timestampTz(timeZone.get(), isNanosecond);
            }
          }
          // TODO(aduffy): handle vortex.time extension type (not used by TPC-H data)

          throw new UnsupportedOperationException("Unsupported Vortex Extension type in schema");
        default:
          throw new UnsupportedOperationException(
              "Unsupported Vortex type: " + vortexType.getVariant());
      }
    }
  }
}
