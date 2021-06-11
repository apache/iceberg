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
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcSchemaWithTypeVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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

  private final SparkOrcValueWriter writer;

  public SparkOrcWriter(Schema iSchema, TypeDescription orcSchema) {
    Preconditions.checkArgument(orcSchema.getCategory() == TypeDescription.Category.STRUCT,
        "Top level must be a struct " + orcSchema);

    writer = OrcSchemaWithTypeVisitor.visit(iSchema, orcSchema, new WriteBuilder());
  }

  @Override
  public void write(InternalRow value, VectorizedRowBatch output) {
    Preconditions.checkArgument(writer instanceof StructWriter, "writer must be StructWriter");

    int row = output.size;
    output.size += 1;
    List<SparkOrcValueWriter> writers = ((StructWriter) writer).writers();
    for (int c = 0; c < writers.size(); c++) {
      SparkOrcValueWriter child = writers.get(c);
      child.write(row, c, value, output.cols[c]);
    }
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    return writer.metrics();
  }

  private static class WriteBuilder extends OrcSchemaWithTypeVisitor<SparkOrcValueWriter> {
    private WriteBuilder() {
    }

    @Override
    public SparkOrcValueWriter record(Types.StructType iStruct, TypeDescription record,
                                      List<String> names, List<SparkOrcValueWriter> fields) {
      return new StructWriter(fields);
    }

    @Override
    public SparkOrcValueWriter list(Types.ListType iList, TypeDescription array,
                                    SparkOrcValueWriter element) {
      return SparkOrcValueWriters.list(element);
    }

    @Override
    public SparkOrcValueWriter map(Types.MapType iMap, TypeDescription map,
                                   SparkOrcValueWriter key, SparkOrcValueWriter value) {
      return SparkOrcValueWriters.map(key, value);
    }

    @Override
    public SparkOrcValueWriter primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
      switch (primitive.getCategory()) {
        case BOOLEAN:
          return SparkOrcValueWriters.booleans();
        case BYTE:
          return SparkOrcValueWriters.bytes();
        case SHORT:
          return SparkOrcValueWriters.shorts();
        case DATE:
        case INT:
          return SparkOrcValueWriters.ints();
        case LONG:
          return SparkOrcValueWriters.longs();
        case FLOAT:
          return SparkOrcValueWriters.floats(ORCSchemaUtil.fieldId(primitive));
        case DOUBLE:
          return SparkOrcValueWriters.doubles(ORCSchemaUtil.fieldId(primitive));
        case BINARY:
          return SparkOrcValueWriters.byteArrays();
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

  private static class StructWriter implements SparkOrcValueWriter {
    private final List<SparkOrcValueWriter> writers;

    StructWriter(List<SparkOrcValueWriter> writers) {
      this.writers = writers;
    }

    List<SparkOrcValueWriter> writers() {
      return writers;
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      InternalRow value = data.getStruct(column, writers.size());
      StructColumnVector cv = (StructColumnVector) output;
      for (int c = 0; c < writers.size(); ++c) {
        writers.get(c).write(rowId, c, value, cv.fields[c]);
      }
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return writers.stream().flatMap(SparkOrcValueWriter::metrics);
    }

  }
}
