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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.vortex.VortexRowReader;
import org.apache.iceberg.vortex.VortexSchemaWithTypeVisitor;
import org.apache.iceberg.vortex.VortexSchemas;
import org.apache.iceberg.vortex.VortexValueReader;

public class GenericVortexReader implements VortexRowReader<Record> {
  private final Types.StructType structType;
  private final List<VortexValueReader<?>> fieldReaders;

  private GenericVortexReader(
      Schema expectedSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      Map<Integer, ?> idToConstant) {
    this.structType = expectedSchema.asStruct();
    GenericReadBuilder builder = new GenericReadBuilder(idToConstant);
    List<Field> fileFields = fileArrowSchema.getFields();
    List<Types.NestedField> expectedFields = structType.fields();
    this.fieldReaders = Lists.newArrayListWithExpectedSize(expectedFields.size());
    for (int i = 0; i < expectedFields.size(); i++) {
      Type icebergType = expectedFields.get(i).type();
      Field arrowField = fileFields.get(i);
      this.fieldReaders.add(VortexSchemaWithTypeVisitor.visit(icebergType, arrowField, builder));
    }
  }

  public static VortexRowReader<Record> buildReader(
      Schema expectedSchema, org.apache.arrow.vector.types.pojo.Schema fileArrowSchema) {
    return new GenericVortexReader(expectedSchema, fileArrowSchema, Collections.emptyMap());
  }

  public static VortexRowReader<Record> buildReader(
      Schema expectedSchema,
      org.apache.arrow.vector.types.pojo.Schema fileArrowSchema,
      Map<Integer, ?> idToConstant) {
    return new GenericVortexReader(expectedSchema, fileArrowSchema, idToConstant);
  }

  @Override
  public Record read(VectorSchemaRoot batch, int row) {
    GenericRecord record = GenericRecord.create(structType);
    for (int i = 0; i < fieldReaders.size(); i++) {
      VortexValueReader<?> reader = fieldReaders.get(i);
      FieldVector vector = batch.getVector(i);
      record.set(i, reader.read(vector, row));
    }
    return record;
  }

  @SuppressWarnings("UnusedVariable")
  static class GenericReadBuilder extends VortexSchemaWithTypeVisitor<VortexValueReader<?>> {
    // TODO(aduffy): implement constant readers to fill in identity partition values
    private final Map<Integer, ?> idToConstant;

    GenericReadBuilder(Map<Integer, ?> idToConstant) {
      this.idToConstant = idToConstant;
    }

    @Override
    public VortexValueReader<?> struct(
        Types.StructType iStruct, List<Field> fields, List<VortexValueReader<?>> children) {
      return GenericVortexReaders.struct(iStruct, children);
    }

    @Override
    public VortexValueReader<?> list(
        Types.ListType iList, Field listField, VortexValueReader<?> element) {
      throw new UnsupportedOperationException("LIST TYPES!");
    }

    @Override
    public VortexValueReader<?> primitive(Type.PrimitiveType iPrimitive, Field primField) {
      if ((iPrimitive != null && iPrimitive.typeId() == Type.TypeID.UUID)
          || VortexSchemas.isUuidField(primField)) {
        return GenericVortexReaders.uuids();
      }
      ArrowType arrowType = primField.getType();
      if (arrowType instanceof ArrowType.Int intType) {
        return intType.getBitWidth() <= Integer.SIZE
            ? GenericVortexReaders.ints()
            : GenericVortexReaders.longs();
      } else if (arrowType instanceof ArrowType.FloatingPoint fpType) {
        return floatingPointReader(fpType);
      } else if (arrowType instanceof ArrowType.Date dateType) {
        return GenericVortexReaders.date(dateType.getUnit() == DateUnit.MILLISECOND);
      } else if (arrowType instanceof ArrowType.Time timeType) {
        return GenericVortexReaders.time(timeType.getUnit() == TimeUnit.NANOSECOND);
      } else if (arrowType instanceof ArrowType.Timestamp tsType) {
        return timestampReader(tsType);
      }
      return simpleReader(arrowType);
    }

    private static VortexValueReader<?> simpleReader(ArrowType arrowType) {
      if (arrowType instanceof ArrowType.Bool) {
        return GenericVortexReaders.bools();
      } else if (arrowType instanceof ArrowType.Decimal) {
        return GenericVortexReaders.decimals();
      } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
        return GenericVortexReaders.strings();
      } else if (arrowType instanceof ArrowType.Binary
          || arrowType instanceof ArrowType.LargeBinary
          || arrowType instanceof ArrowType.FixedSizeBinary) {
        return GenericVortexReaders.bytes();
      }
      throw new UnsupportedOperationException(
          "Unsupported Arrow type in Vortex read: " + arrowType);
    }

    private static VortexValueReader<?> floatingPointReader(ArrowType.FloatingPoint fpType) {
      return switch (fpType.getPrecision()) {
        case SINGLE -> GenericVortexReaders.floats();
        case DOUBLE -> GenericVortexReaders.doubles();
        case HALF ->
            throw new UnsupportedOperationException("Half-precision floats are not supported");
      };
    }

    private static VortexValueReader<?> timestampReader(ArrowType.Timestamp tsType) {
      boolean isNano = tsType.getUnit() == TimeUnit.NANOSECOND;
      if (tsType.getTimezone() == null) {
        return GenericVortexReaders.timestamp(isNano);
      }
      return GenericVortexReaders.timestampTz(tsType.getTimezone(), isNano);
    }
  }
}
