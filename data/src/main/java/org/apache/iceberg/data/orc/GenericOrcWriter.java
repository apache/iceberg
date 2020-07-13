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

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericOrcWriter implements OrcValueWriter<Record> {
  private final GenericOrcWriters.Converter converter;

  private GenericOrcWriter(Schema expectedSchema, TypeDescription orcSchema) {
    Preconditions.checkArgument(orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "Top level must be a struct " + orcSchema);

    converter = OrcSchemaWithTypeVisitor.visit(expectedSchema, orcSchema, new WriteBuilder());
  }

  public static OrcValueWriter<Record> buildWriter(Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericOrcWriter(expectedSchema, fileSchema);
  }

  private static class WriteBuilder extends OrcSchemaWithTypeVisitor<GenericOrcWriters.Converter> {
    private WriteBuilder() {
    }

    public GenericOrcWriters.Converter record(Types.StructType iStruct, TypeDescription record,
                                              List<String> names, List<GenericOrcWriters.Converter> fields) {
      return new GenericOrcWriters.RecordConverter(fields);
    }

    public GenericOrcWriters.Converter list(Types.ListType iList, TypeDescription array,
                                            GenericOrcWriters.Converter element) {
      return new GenericOrcWriters.ListConverter(element);
    }

    public GenericOrcWriters.Converter map(Types.MapType iMap, TypeDescription map,
                                           GenericOrcWriters.Converter key, GenericOrcWriters.Converter value) {
      return new GenericOrcWriters.MapConverter(key, value);
    }

    public GenericOrcWriters.Converter primitive(Type.PrimitiveType iPrimitive, TypeDescription schema) {
      switch (schema.getCategory()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case BYTE:
          return GenericOrcWriters.bytes();
        case SHORT:
          return GenericOrcWriters.shorts();
        case DATE:
          return GenericOrcWriters.dates();
        case INT:
          return GenericOrcWriters.ints();
        case LONG:
          String longAttributeValue = schema.getAttributeValue(ORCSchemaUtil.ICEBERG_LONG_TYPE_ATTRIBUTE);
          ORCSchemaUtil.LongType longType = longAttributeValue == null ? ORCSchemaUtil.LongType.LONG :
              ORCSchemaUtil.LongType.valueOf(longAttributeValue);
          switch (longType) {
            case TIME:
              return GenericOrcWriters.times();
            case LONG:
              return GenericOrcWriters.longs();
            default:
              throw new IllegalStateException("Unhandled Long type found in ORC type attribute: " + longType);
          }
        case FLOAT:
          return GenericOrcWriters.floats();
        case DOUBLE:
          return GenericOrcWriters.doubles();
        case BINARY:
          String binaryAttributeValue = schema.getAttributeValue(ORCSchemaUtil.ICEBERG_BINARY_TYPE_ATTRIBUTE);
          ORCSchemaUtil.BinaryType binaryType = binaryAttributeValue == null ? ORCSchemaUtil.BinaryType.BINARY :
              ORCSchemaUtil.BinaryType.valueOf(binaryAttributeValue);
          switch (binaryType) {
            case UUID:
              return GenericOrcWriters.uuids();
            case FIXED:
              return GenericOrcWriters.fixed();
            case BINARY:
              return GenericOrcWriters.binary();
            default:
              throw new IllegalStateException("Unhandled Binary type found in ORC type attribute: " + binaryType);
          }
        case STRING:
        case CHAR:
        case VARCHAR:
          return GenericOrcWriters.strings();
        case DECIMAL:
          return schema.getPrecision() <= 18 ? GenericOrcWriters.decimal18(schema) :
              GenericOrcWriters.decimal38(schema);
        case TIMESTAMP:
          return GenericOrcWriters.timestamp();
        case TIMESTAMP_INSTANT:
          return GenericOrcWriters.timestampTz();
      }
      throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Record value, VectorizedRowBatch output) {
    Preconditions.checkArgument(converter instanceof GenericOrcWriters.RecordConverter,
        "Converter must be a RecordConverter.");

    int row = output.size++;
    List<GenericOrcWriters.Converter> converters = ((GenericOrcWriters.RecordConverter) converter).converters();
    for (int c = 0; c < converters.size(); ++c) {
      converters.get(c).addValue(row, value.get(c, converters.get(c).getJavaClass()), output.cols[c]);
    }
  }
}
