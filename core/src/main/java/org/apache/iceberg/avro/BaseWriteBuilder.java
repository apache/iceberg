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
package org.apache.iceberg.avro;

import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

abstract class BaseWriteBuilder extends AvroSchemaVisitor<ValueWriter<?>> {

  protected abstract ValueWriter<?> createRecordWriter(List<ValueWriter<?>> fields);

  protected abstract ValueWriter<?> fixedWriter(int length);

  @Override
  public ValueWriter<?> record(Schema record, List<String> names, List<ValueWriter<?>> fields) {
    return createRecordWriter(fields);
  }

  @Override
  public ValueWriter<?> union(Schema union, List<ValueWriter<?>> options) {
    Preconditions.checkArgument(
        options.size() == 2, "Cannot create writer for non-option union: %s", union);
    if (union.getTypes().get(0).getType() == Schema.Type.NULL) {
      return ValueWriters.option(0, options.get(1));
    } else if (union.getTypes().get(1).getType() == Schema.Type.NULL) {
      return ValueWriters.option(1, options.get(0));
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot create writer for non-option union: %s", union));
    }
  }

  @Override
  public ValueWriter<?> array(Schema array, ValueWriter<?> elementWriter) {
    if (array.getLogicalType() instanceof LogicalMap) {
      ValueWriters.StructWriter<?> keyValueWriter = (ValueWriters.StructWriter<?>) elementWriter;
      return ValueWriters.arrayMap(keyValueWriter.writer(0), keyValueWriter.writer(1));
    }

    return ValueWriters.array(elementWriter);
  }

  @Override
  public ValueWriter<?> map(Schema map, ValueWriter<?> valueWriter) {
    return ValueWriters.map(ValueWriters.strings(), valueWriter);
  }

  @Override
  public ValueWriter<?> variant(
      Schema variant, ValueWriter<?> metadataResult, ValueWriter<?> valueResult) {
    return ValueWriters.variants();
  }

  @Override
  public ValueWriter<?> primitive(Schema primitive) {
    LogicalType logicalType = primitive.getLogicalType();
    if (logicalType != null) {
      return switch (logicalType.getName()) {
        case "date" -> ValueWriters.ints();
        case "time-micros" -> ValueWriters.longs();
        case "timestamp-micros" -> ValueWriters.longs();
        case "timestamp-nanos" -> ValueWriters.longs();
        case "decimal" -> {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
          yield ValueWriters.decimal(decimal.getPrecision(), decimal.getScale());
        }
        case "uuid" -> ValueWriters.uuids();
        default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
      };
    }

    return switch (primitive.getType()) {
      case NULL -> ValueWriters.nulls();
      case BOOLEAN -> ValueWriters.booleans();
      case INT -> ValueWriters.ints();
      case LONG -> ValueWriters.longs();
      case FLOAT -> ValueWriters.floats();
      case DOUBLE -> ValueWriters.doubles();
      case STRING -> ValueWriters.strings();
      case FIXED -> fixedWriter(primitive.getFixedSize());
      case BYTES -> ValueWriters.byteBuffers();
      default -> throw new IllegalArgumentException("Unsupported type: " + primitive);
    };
  }
}
