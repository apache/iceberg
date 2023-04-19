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
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericOrcWriter implements OrcRowWriter<Record> {
  private final RecordWriter writer;

  private GenericOrcWriter(Schema expectedSchema, TypeDescription orcSchema) {
    Preconditions.checkArgument(
        orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "Top level must be a struct " + orcSchema);

    writer =
        (RecordWriter)
            OrcSchemaWithTypeVisitor.visit(expectedSchema, orcSchema, new WriteBuilder());
  }

  public static OrcRowWriter<Record> buildWriter(
      Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericOrcWriter(expectedSchema, fileSchema);
  }

  private static class WriteBuilder extends OrcSchemaWithTypeVisitor<OrcValueWriter<?>> {
    private WriteBuilder() {}

    @Override
    public OrcValueWriter<Record> record(
        Types.StructType iStruct,
        TypeDescription record,
        List<String> names,
        List<OrcValueWriter<?>> fields) {
      return new RecordWriter(fields);
    }

    @Override
    public OrcValueWriter<?> list(
        Types.ListType iList, TypeDescription array, OrcValueWriter<?> element) {
      return GenericOrcWriters.list(element);
    }

    @Override
    public OrcValueWriter<?> map(
        Types.MapType iMap, TypeDescription map, OrcValueWriter<?> key, OrcValueWriter<?> value) {
      return GenericOrcWriters.map(key, value);
    }

    @Override
    public OrcValueWriter<?> primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (iPrimitive.typeId()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case INTEGER:
          return GenericOrcWriters.ints();
        case LONG:
          return GenericOrcWriters.longs();
        case FLOAT:
          return GenericOrcWriters.floats(ORCSchemaUtil.fieldId(primitive));
        case DOUBLE:
          return GenericOrcWriters.doubles(ORCSchemaUtil.fieldId(primitive));
        case DATE:
          return GenericOrcWriters.dates();
        case TIME:
          return GenericOrcWriters.times();
        case TIMESTAMP:
          Types.TimestampType timestampType = (Types.TimestampType) iPrimitive;
          if (timestampType.shouldAdjustToUTC()) {
            return GenericOrcWriters.timestampTz();
          } else {
            return GenericOrcWriters.timestamp();
          }
        case STRING:
          return GenericOrcWriters.strings();
        case UUID:
          return GenericOrcWriters.uuids();
        case FIXED:
          return GenericOrcWriters.byteArrays();
        case BINARY:
          return GenericOrcWriters.byteBuffers();
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) iPrimitive;
          return GenericOrcWriters.decimal(decimalType.precision(), decimalType.scale());
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Invalid iceberg type %s corresponding to ORC type %s", iPrimitive, primitive));
      }
    }
  }

  @Override
  public void write(Record value, VectorizedRowBatch output) {
    Preconditions.checkArgument(value != null, "value must not be null");
    writer.writeRow(value, output);
  }

  @Override
  public List<OrcValueWriter<?>> writers() {
    return writer.writers();
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    return writer.metrics();
  }

  private static class RecordWriter extends GenericOrcWriters.StructWriter<Record> {

    RecordWriter(List<OrcValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Record struct, int index) {
      return struct.get(index);
    }
  }
}
