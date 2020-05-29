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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkAvroWriter implements DatumWriter<InternalRow> {
  private final StructType dsSchema;
  private ValueWriter<InternalRow> writer = null;

  public SparkAvroWriter(StructType dsSchema) {
    this.dsSchema = dsSchema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema schema) {
    this.writer = (ValueWriter<InternalRow>) AvroWithSparkSchemaVisitor
        .visit(dsSchema, schema, new WriteBuilder());
  }

  @Override
  public void write(InternalRow datum, Encoder out) throws IOException {
    writer.write(datum, out);
  }

  private static class WriteBuilder extends AvroWithSparkSchemaVisitor<ValueWriter<?>> {
    @Override
    public ValueWriter<?> record(StructType struct, Schema record, List<String> names, List<ValueWriter<?>> fields) {
      List<DataType> types = Stream.of(struct.fields()).map(StructField::dataType).collect(Collectors.toList());
      return SparkValueWriters.struct(fields, types);
    }

    @Override
    public ValueWriter<?> union(DataType type, Schema union, List<ValueWriter<?>> options) {
      Preconditions.checkArgument(options.contains(ValueWriters.nulls()),
          "Cannot create writer for non-option union: %s", union);
      Preconditions.checkArgument(options.size() == 2,
          "Cannot create writer for non-option union: %s", union);
      if (union.getTypes().get(0).getType() == Schema.Type.NULL) {
        return ValueWriters.option(0, options.get(1));
      } else {
        return ValueWriters.option(1, options.get(0));
      }
    }

    @Override
    public ValueWriter<?> array(ArrayType sArray, Schema array, ValueWriter<?> elementWriter) {
      return SparkValueWriters.array(elementWriter, sArray.elementType());
    }

    @Override
    public ValueWriter<?> map(MapType sMap, Schema map, ValueWriter<?> valueReader) {
      return SparkValueWriters.map(SparkValueWriters.strings(), sMap.keyType(), valueReader, sMap.valueType());
    }

    @Override
    public ValueWriter<?> map(MapType sMap, Schema map, ValueWriter<?> keyWriter, ValueWriter<?> valueWriter) {
      return SparkValueWriters.arrayMap(keyWriter, sMap.keyType(), valueWriter, sMap.valueType());
    }

    @Override
    public ValueWriter<?> primitive(DataType type, Schema primitive) {
      LogicalType logicalType = primitive.getLogicalType();
      if (logicalType != null) {
        switch (logicalType.getName()) {
          case "date":
            // Spark uses the same representation
            return ValueWriters.ints();

          case "timestamp-micros":
            // Spark uses the same representation
            return ValueWriters.longs();

          case "decimal":
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            return SparkValueWriters.decimal(decimal.getPrecision(), decimal.getScale());

          case "uuid":
            return ValueWriters.uuids();

          default:
            throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        }
      }

      switch (primitive.getType()) {
        case NULL:
          return ValueWriters.nulls();
        case BOOLEAN:
          return ValueWriters.booleans();
        case INT:
          if (type instanceof ByteType) {
            return ValueWriters.tinyints();
          } else if (type instanceof ShortType) {
            return ValueWriters.shorts();
          }
          return ValueWriters.ints();
        case LONG:
          return ValueWriters.longs();
        case FLOAT:
          return ValueWriters.floats();
        case DOUBLE:
          return ValueWriters.doubles();
        case STRING:
          return SparkValueWriters.strings();
        case FIXED:
          return ValueWriters.fixed(primitive.getFixedSize());
        case BYTES:
          return ValueWriters.bytes();
        default:
          throw new IllegalArgumentException("Unsupported type: " + primitive);
      }
    }
  }
}
