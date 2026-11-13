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
package org.apache.iceberg.tornadovm;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.MessageType;
import uk.ac.manchester.tornado.cudf.enums.CudfType;

/**
 * Where each projected column lives on the device: which typed buffer, and which slot of it.
 *
 * <p>cuDF writes the k-th column of a physical type at element {@code k * capacity} of that type's
 * buffer. The order of the read is chosen so that the columns a library call reads from offset 0
 * are slot 0: the equality-delete key first (the set probe reads it), then the group key.
 */
final class ScanLayout {

  private final List<Types.NestedField> fields = Lists.newArrayList();
  private final List<CudfType> types = Lists.newArrayList();
  // What the read asks the shim for: FLOAT32 for a float column, which is stored as FP64.
  private final List<CudfType> readTypes = Lists.newArrayList();
  private final Map<Integer, Integer> slotById = Maps.newHashMap();
  private final Map<Integer, Integer> readIndexById = Maps.newHashMap();
  private final int[] slotsPerType = new int[3];
  private final List<Types.NestedField> strings = Lists.newArrayList();
  private int numericCount;
  private boolean anyOptional = false;

  ScanLayout(Schema schema, Integer equalityKeyId, String groupColumn, List<String> columns) {
    if (equalityKeyId != null) {
      add(schema.findField(equalityKeyId));
    }
    if (groupColumn != null) {
      add(field(schema, groupColumn));
    }
    for (String column : columns) {
      add(field(schema, column));
    }
    // STRING columns come after every numeric one: the numeric read fills the first slots of
    // each buffer and the first validity rows, the string read and its packing kernel the rest.
    // Each string takes an INT64 slot, packed (see packStrings).
    numericCount = fields.size();
    for (Types.NestedField field : strings) {
      readIndexById.put(field.fieldId(), fields.size());
      slotById.put(field.fieldId(), slotsPerType[CudfType.INT64.code()]++);
      fields.add(field);
      types.add(CudfType.INT64);
      anyOptional |= field.isOptional();
    }
  }

  private static Types.NestedField field(Schema schema, String name) {
    Types.NestedField field = schema.findField(name);
    if (field == null) {
      throw new IllegalArgumentException("No column " + name + " in " + schema);
    }
    return field;
  }

  private void add(Types.NestedField field) {
    if (slotById.containsKey(field.fieldId()) || strings.contains(field)) {
      return;
    }
    if (field.type().typeId() == Type.TypeID.STRING) {
      strings.add(field);
      return;
    }
    CudfType type = cudfType(field);
    readIndexById.put(field.fieldId(), fields.size());
    slotById.put(field.fieldId(), slotsPerType[type.code()]++);
    fields.add(field);
    types.add(type);
    readTypes.add(
        switch (field.type().typeId()) {
          case FLOAT -> CudfType.FLOAT32;
          case DECIMAL -> CudfType.DECIMAL64;
          default -> type;
        });
    anyOptional |= field.isOptional();
  }

  /** The device type of an Iceberg primitive, or a refusal: nothing here is ever cast. */
  static CudfType cudfType(Types.NestedField field) {
    Type.TypeID id = field.type().typeId();
    return switch (id) {
      case INTEGER, DATE -> CudfType.INT32;
      case LONG, TIMESTAMP, TIME -> CudfType.INT64;
        // Strings of up to 8 bytes, packed into an INT64 on the device; longer ones are refused
        // when
        // a batch meets one.
      case STRING -> CudfType.INT64;
        // A float is read widened, exactly, to FP64 (CudfType.FLOAT32), so the device keeps one
        // floating-point buffer and every kernel sees a double.
      case DOUBLE, FLOAT -> CudfType.FLOAT64;
        // A decimal of up to 18 digits as its unscaled value (CudfType.DECIMAL64); its scale is the
        // column's.
      case DECIMAL -> {
        if (((Types.DecimalType) field.type()).precision() > MAX_DECIMAL_PRECISION) {
          throw new UnsupportedOperationException(
              "Column " + field.name() + " of type " + field.type() + " has over 18 digits");
        }
        yield CudfType.INT64;
      }
      default ->
          throw new UnsupportedOperationException(
              "Column "
                  + field.name()
                  + " of type "
                  + field.type()
                  + " cannot be read on the device");
    };
  }

  /** The most digits a decimal column may have: its unscaled value is an INT64. */
  static final int MAX_DECIMAL_PRECISION = 18;

  /**
   * A value of a column as the device holds it in an INT64 or INT32 slot: a decimal as its unscaled
   * value at the column's scale (exactly, or not at all), any other number as itself.
   */
  static long deviceLong(Types.NestedField field, Object value) {
    if (value instanceof BigDecimal decimal) {
      int scale = ((Types.DecimalType) field.type()).scale();
      return decimal.setScale(scale).unscaledValue().longValueExact();
    }
    return ((Number) value).longValue();
  }

  List<Types.NestedField> fields() {
    return fields;
  }

  /** The types of the numeric columns, which the numeric read takes: the first fields. */
  CudfType[] types() {
    return readTypes.subList(0, numericCount).toArray(new CudfType[0]);
  }

  /** How many fields are numeric; the string fields follow them. */
  int numericCount() {
    return numericCount;
  }

  int stringCount() {
    return fields.size() - numericCount;
  }

  /** The INT64 slot of the first string column; the others follow it. */
  int firstStringSlot() {
    return slotsPerType[CudfType.INT64.code()] - stringCount();
  }

  boolean isString(int fieldId) {
    return readIndex(fieldId) >= numericCount;
  }

  /**
   * A string of up to 8 UTF-8 bytes as the device holds it: big-endian, zero-padded, sign bit
   * flipped, so that signed INT64 order is the strings' byte order. Refuses a longer one, which
   * would compare wrongly once cut.
   */
  static long packString(CharSequence value) {
    byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 8) {
      throw new UnsupportedOperationException(
          "Strings of more than 8 bytes are not supported on the device: " + value);
    }
    long packed = 0;
    for (int b = 0; b < 8; b++) {
      packed = (packed << 8) | (b < bytes.length ? bytes[b] & 0xFFL : 0L);
    }
    return packed ^ Long.MIN_VALUE;
  }

  /** The string {@link #packString} packed. */
  static String unpackString(long packed) {
    long raw = packed ^ Long.MIN_VALUE;
    byte[] bytes = new byte[8];
    int length = 0;
    for (int b = 0; b < 8; b++) {
      bytes[b] = (byte) (raw >>> (56 - 8 * b));
      if (bytes[b] != 0) {
        length = b + 1;
      }
    }
    return new String(bytes, 0, length, StandardCharsets.UTF_8);
  }

  int slots(CudfType type) {
    return slotsPerType[type.code()];
  }

  int slot(int fieldId) {
    return slotById.get(fieldId);
  }

  /** The column's position in the read, which is also its slot in the validity bytes. */
  int readIndex(int fieldId) {
    return readIndexById.get(fieldId);
  }

  /** What the read asks the shim for, for this column: see {@link #types()}. */
  CudfType readType(int fieldId) {
    return readTypes.get(readIndex(fieldId));
  }

  CudfType type(int fieldId) {
    return types.get(readIndex(fieldId));
  }

  boolean anyOptional() {
    return anyOptional;
  }

  /**
   * The schema index of each projected column in one data file, resolved by Iceberg field id the
   * way Iceberg's own readers resolve them, so renamed and reordered columns read correctly.
   */
  int[] parquetColumns(MessageType fileSchema, String location) {
    int[] columns = new int[fields.size()];
    for (int c = 0; c < fields.size(); c++) {
      Types.NestedField field = fields.get(c);
      columns[c] = -1;
      for (int i = 0; i < fileSchema.getFieldCount(); i++) {
        org.apache.parquet.schema.Type column = fileSchema.getType(i);
        if (column.getId() != null && column.getId().intValue() == field.fieldId()) {
          columns[c] = i;
          break;
        }
      }
      if (columns[c] < 0
          && (field.isRequired()
              || field.initialDefault() != null
              || field.type().typeId() == Type.TypeID.STRING)) {
        // A column added after the file was written reads as null, which the device read does
        // (index -1); a required one or one with a default value needs what it cannot produce.
        throw new UnsupportedOperationException(
            "Data file " + location + " has no column with field id " + field.fieldId());
      }
    }
    return columns;
  }
}
