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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.avro.AvroSchemaVisitor;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkAvroWriter implements DatumWriter<InternalRow> {
  private final org.apache.iceberg.Schema schema;
  private ValueWriter<InternalRow> writer = null;

  public SparkAvroWriter(org.apache.iceberg.Schema schema) {
    this.schema = schema;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setSchema(Schema schema) {
    this.writer = (ValueWriter<InternalRow>) AvroSchemaVisitor.visit(schema, new WriteBuilder(this.schema));
  }

  @Override
  public void write(InternalRow datum, Encoder out) throws IOException {
    writer.write(datum, out);
  }

  private static class WriteBuilder extends AvroSchemaVisitor<ValueWriter<?>> {
    private final org.apache.iceberg.Schema schema;

    private WriteBuilder(org.apache.iceberg.Schema schema) {
      this.schema = schema;
    }

    @Override
    public ValueWriter<?> record(Schema record, List<String> names, List<ValueWriter<?>> fields) {
      List<DataType> types = Lists.newArrayList();
      for (Schema.Field field : record.getFields()) {
        int fieldId = AvroSchemaUtil.getFieldId(field);
        Type fieldType = schema.findType(fieldId);
        types.add(SparkSchemaUtil.convert(fieldType));
      }
      return SparkValueWriters.struct(fields, types);
    }

    @Override
    public ValueWriter<?> union(Schema union, List<ValueWriter<?>> options) {
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
    public ValueWriter<?> array(Schema array, ValueWriter<?> elementWriter) {
      LogicalType logical = array.getLogicalType();
      if (logical != null && "map".equals(logical.getName())) {
        int keyFieldId = AvroSchemaUtil.getFieldId(array.getElementType().getField("key"));
        Type keyType = schema.findType(keyFieldId);
        int valueFieldId = AvroSchemaUtil.getFieldId(array.getElementType().getField("value"));
        Type valueType = schema.findType(valueFieldId);
        ValueWriter<?>[] writers = ((SparkValueWriters.StructWriter) elementWriter).writers();
        return SparkValueWriters.arrayMap(
            writers[0], SparkSchemaUtil.convert(keyType), writers[1], SparkSchemaUtil.convert(valueType));
      }

      Type elementType = schema.findType(AvroSchemaUtil.getElementId(array));
      return SparkValueWriters.array(elementWriter, SparkSchemaUtil.convert(elementType));
    }

    @Override
    public ValueWriter<?> map(Schema map, ValueWriter<?> valueReader) {
      Type keyType = schema.findType(AvroSchemaUtil.getKeyId(map));
      Type valueType = schema.findType(AvroSchemaUtil.getValueId(map));
      ValueWriter<UTF8String> writer = SparkValueWriters.strings();
      return SparkValueWriters.map(
          writer, SparkSchemaUtil.convert(keyType), valueReader, SparkSchemaUtil.convert(valueType));
    }

    @Override
    public ValueWriter<?> primitive(Schema primitive) {
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
