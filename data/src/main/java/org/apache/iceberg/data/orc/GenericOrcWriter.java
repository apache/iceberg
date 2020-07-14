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
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class GenericOrcWriter implements OrcRowWriter<Record> {
  private final OrcValueWriter orcValueWriter;

  private GenericOrcWriter(Schema expectedSchema, TypeDescription orcSchema) {
    Preconditions.checkArgument(orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "Top level must be a struct " + orcSchema);

    orcValueWriter = OrcSchemaWithTypeVisitor.visit(expectedSchema, orcSchema, new WriteBuilder());
  }

  public static OrcRowWriter<Record> buildWriter(Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericOrcWriter(expectedSchema, fileSchema);
  }

  private static class WriteBuilder extends OrcSchemaWithTypeVisitor<OrcValueWriter> {
    private WriteBuilder() {
    }

    @Override
    public OrcValueWriter<Record> record(Types.StructType iStruct, TypeDescription record,
                                         List<String> names, List<OrcValueWriter> fields) {
      return new RecordOrcValueWriter(fields);
    }

    @Override
    public OrcValueWriter<List> list(Types.ListType iList, TypeDescription array,
                                     OrcValueWriter element) {
      return GenericOrcWriters.list(element);
    }

    @Override
    public OrcValueWriter<Map> map(Types.MapType iMap, TypeDescription map,
                                   OrcValueWriter key, OrcValueWriter value) {
      return GenericOrcWriters.map(key, value);
    }

    @Override
    public OrcValueWriter primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (primitive.getCategory()) {
        case BOOLEAN:
          return GenericOrcWriters.booleans();
        case BYTE:
          throw new IllegalArgumentException("Iceberg does not have a byte type");
        case SHORT:
          throw new IllegalArgumentException("Iceberg does not have a short type.");
        case INT:
          return GenericOrcWriters.ints();
        case LONG:
          switch (iPrimitive.typeId()) {
            case TIME:
              return GenericOrcWriters.times();
            case LONG:
              return GenericOrcWriters.longs();
            default:
              throw new IllegalStateException(
                  String.format("Invalid iceberg type %s corresponding to ORC type %s", iPrimitive, primitive));
          }
        case FLOAT:
          return GenericOrcWriters.floats();
        case DOUBLE:
          return GenericOrcWriters.doubles();
        case DATE:
          return GenericOrcWriters.dates();
        case TIMESTAMP:
          return GenericOrcWriters.timestamp();
        case TIMESTAMP_INSTANT:
          return GenericOrcWriters.timestampTz();
        case DECIMAL:
          return GenericOrcWriters.decimal(primitive.getScale(), primitive.getPrecision());
        case CHAR:
        case VARCHAR:
        case STRING:
          return GenericOrcWriters.strings();
        case BINARY:
          switch (iPrimitive.typeId()) {
            case UUID:
              return GenericOrcWriters.uuids();
            case FIXED:
              return GenericOrcWriters.fixed();
            case BINARY:
              return GenericOrcWriters.binary();
            default:
              throw new IllegalStateException(
                  String.format("Invalid iceberg type %s corresponding to ORC type %s", iPrimitive, primitive));
          }
        default:
          throw new IllegalArgumentException("Unhandled type " + primitive);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Record value, VectorizedRowBatch output) {
    Preconditions.checkArgument(orcValueWriter instanceof RecordOrcValueWriter,
        "Converter must be a RecordConverter.");

    int row = output.size;
    output.size += 1;
    List<OrcValueWriter> orcValueWriters = ((RecordOrcValueWriter) orcValueWriter).converters();
    for (int c = 0; c < orcValueWriters.size(); ++c) {
      orcValueWriters.get(c).addValue(row, value.get(c, orcValueWriters.get(c).getJavaClass()), output.cols[c]);
    }
  }

  private static class RecordOrcValueWriter implements OrcValueWriter<Record> {
    private final List<OrcValueWriter> orcValueWriters;

    RecordOrcValueWriter(List<OrcValueWriter> orcValueWriters) {
      this.orcValueWriters = orcValueWriters;
    }

    List<OrcValueWriter> converters() {
      return orcValueWriters;
    }

    @Override
    public Class<Record> getJavaClass() {
      return Record.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int rowId, Record data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        StructColumnVector cv = (StructColumnVector) output;
        for (int c = 0; c < orcValueWriters.size(); ++c) {
          orcValueWriters.get(c).addValue(rowId, data.get(c, orcValueWriters.get(c).getJavaClass()), cv.fields[c]);
        }
      }
    }
  }
}
