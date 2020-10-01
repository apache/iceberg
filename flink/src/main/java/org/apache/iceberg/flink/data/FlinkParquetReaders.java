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

package org.apache.iceberg.flink.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ReusableArrayData;
import org.apache.iceberg.parquet.ReusableMapData;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class FlinkParquetReaders {
  private FlinkParquetReaders() {
  }

  public static ParquetValueReader<RowData> buildReader(Schema expectedSchema, MessageType fileSchema) {
    return buildReader(expectedSchema, fileSchema, ImmutableMap.of());
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<RowData> buildReader(Schema expectedSchema,
                                                        MessageType fileSchema,
                                                        Map<Integer, ?> idToConstant) {
    return (ParquetValueReader<RowData>) TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
        new ReadBuilder(fileSchema, idToConstant)
    );
  }

  private static class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    private final MessageType type;
    private final Map<Integer, ?> idToConstant;

    ReadBuilder(MessageType type, Map<Integer, ?> idToConstant) {
      this.type = type;
      this.idToConstant = idToConstant;
    }

    @Override
    public ParquetValueReader<RowData> message(Types.StructType expected, MessageType message,
                                               List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<RowData> struct(Types.StructType expected, GroupType struct,
                                              List<ParquetValueReader<?>> fieldReaders) {
      // match the expected struct's order
      Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        if (fieldReaders.get(i) != null) {
          int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
          if (fieldType.getId() != null) {
            int id = fieldType.getId().intValue();
            readersById.put(id, ParquetValueReaders.option(fieldType, fieldD, fieldReaders.get(i)));
            typesById.put(id, fieldType);
          }
        }
      }

      List<Types.NestedField> expectedFields = expected != null ?
          expected.fields() : ImmutableList.of();
      List<ParquetValueReader<?>> reorderedFields = Lists.newArrayListWithExpectedSize(
          expectedFields.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(expectedFields.size());
      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        if (idToConstant.containsKey(id)) {
          // containsKey is used because the constant may be null
          reorderedFields.add(ParquetValueReaders.constant(idToConstant.get(id)));
          types.add(null);
        } else if (id == MetadataColumns.ROW_POSITION.fieldId()) {
          reorderedFields.add(ParquetValueReaders.position());
          types.add(null);
        } else {
          ParquetValueReader<?> reader = readersById.get(id);
          if (reader != null) {
            reorderedFields.add(reader);
            types.add(typesById.get(id));
          } else {
            reorderedFields.add(ParquetValueReaders.nulls());
            types.add(null);
          }
        }
      }

      return new RowDataReader(types, reorderedFields);
    }

    @Override
    public ParquetValueReader<?> list(Types.ListType expectedList, GroupType array,
                                      ParquetValueReader<?> elementReader) {
      if (expectedList == null) {
        return null;
      }

      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName())) - 1;

      return new ArrayReader<>(repeatedD, repeatedR, ParquetValueReaders.option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(Types.MapType expectedMap, GroupType map,
                                     ParquetValueReader<?> keyReader,
                                     ParquetValueReader<?> valueReader) {
      if (expectedMap == null) {
        return null;
      }

      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName())) - 1;
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName())) - 1;

      return new MapReader<>(repeatedD, repeatedR,
          ParquetValueReaders.option(keyType, keyD, keyReader),
          ParquetValueReaders.option(valueType, valueD, valueReader));
    }

    @Override
    @SuppressWarnings("CyclomaticComplexity")
    public ParquetValueReader<?> primitive(org.apache.iceberg.types.Type.PrimitiveType expected,
                                           PrimitiveType primitive) {
      if (expected == null) {
        return null;
      }

      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new StringReader(desc);
          case INT_8:
          case INT_16:
          case INT_32:
            if (expected.typeId() == Types.LongType.get().typeId()) {
              return new ParquetValueReaders.IntAsLongReader(desc);
            } else {
              return new ParquetValueReaders.UnboxedReader<>(desc);
            }
          case TIME_MICROS:
            return new LossyMicrosToMillisTimeReader(desc);
          case TIME_MILLIS:
            return new MillisTimeReader(desc);
          case DATE:
          case INT_64:
            return new ParquetValueReaders.UnboxedReader<>(desc);
          case TIMESTAMP_MICROS:
            if (((Types.TimestampType) expected).shouldAdjustToUTC()) {
              return new MicrosToTimestampTzReader(desc);
            } else {
              return new MicrosToTimestampReader(desc);
            }
          case TIMESTAMP_MILLIS:
            if (((Types.TimestampType) expected).shouldAdjustToUTC()) {
              return new MillisToTimestampTzReader(desc);
            } else {
              return new MillisToTimestampReader(desc);
            }
          case DECIMAL:
            DecimalLogicalTypeAnnotation decimal = (DecimalLogicalTypeAnnotation) primitive.getLogicalTypeAnnotation();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new BinaryDecimalReader(desc, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return new LongDecimalReader(desc, decimal.getPrecision(), decimal.getScale());
              case INT32:
                return new IntegerDecimalReader(desc, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return new ParquetValueReaders.ByteArrayReader(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          return new ParquetValueReaders.ByteArrayReader(desc);
        case INT32:
          if (expected.typeId() == org.apache.iceberg.types.Type.TypeID.LONG) {
            return new ParquetValueReaders.IntAsLongReader(desc);
          } else {
            return new ParquetValueReaders.UnboxedReader<>(desc);
          }
        case FLOAT:
          if (expected.typeId() == org.apache.iceberg.types.Type.TypeID.DOUBLE) {
            return new ParquetValueReaders.FloatAsDoubleReader(desc);
          } else {
            return new ParquetValueReaders.UnboxedReader<>(desc);
          }
        case BOOLEAN:
        case INT64:
        case DOUBLE:
          return new ParquetValueReaders.UnboxedReader<>(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  private static class BinaryDecimalReader extends ParquetValueReaders.PrimitiveReader<DecimalData> {
    private final int precision;
    private final int scale;

    BinaryDecimalReader(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public DecimalData read(DecimalData ignored) {
      Binary binary = column.nextBinary();
      BigDecimal bigDecimal = new BigDecimal(new BigInteger(binary.getBytes()), scale);
      // TODO: need a unit test to write-read-validate decimal via FlinkParquetWrite/Reader
      return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
    }
  }

  private static class IntegerDecimalReader extends ParquetValueReaders.PrimitiveReader<DecimalData> {
    private final int precision;
    private final int scale;

    IntegerDecimalReader(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public DecimalData read(DecimalData ignored) {
      return DecimalData.fromUnscaledLong(column.nextInteger(), precision, scale);
    }
  }

  private static class LongDecimalReader extends ParquetValueReaders.PrimitiveReader<DecimalData> {
    private final int precision;
    private final int scale;

    LongDecimalReader(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public DecimalData read(DecimalData ignored) {
      return DecimalData.fromUnscaledLong(column.nextLong(), precision, scale);
    }
  }

  private static class MicrosToTimestampTzReader extends ParquetValueReaders.UnboxedReader<TimestampData> {
    MicrosToTimestampTzReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public TimestampData read(TimestampData ignored) {
      long value = readLong();
      return TimestampData.fromLocalDateTime(Instant.ofEpochSecond(Math.floorDiv(value, 1000_000),
          Math.floorMod(value, 1000_000) * 1000)
          .atOffset(ZoneOffset.UTC)
          .toLocalDateTime());
    }

    @Override
    public long readLong() {
      return column.nextLong();
    }
  }

  private static class MicrosToTimestampReader extends ParquetValueReaders.UnboxedReader<TimestampData> {
    MicrosToTimestampReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public TimestampData read(TimestampData ignored) {
      long value = readLong();
      return TimestampData.fromInstant(Instant.ofEpochSecond(Math.floorDiv(value, 1000_000),
          Math.floorMod(value, 1000_000) * 1000));
    }

    @Override
    public long readLong() {
      return column.nextLong();
    }
  }

  private static class MillisToTimestampReader extends ParquetValueReaders.UnboxedReader<TimestampData> {
    MillisToTimestampReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public TimestampData read(TimestampData ignored) {
      long millis = readLong();
      return TimestampData.fromEpochMillis(millis);
    }

    @Override
    public long readLong() {
      return column.nextLong();
    }
  }

  private static class MillisToTimestampTzReader extends ParquetValueReaders.UnboxedReader<TimestampData> {
    MillisToTimestampTzReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public TimestampData read(TimestampData ignored) {
      long millis = readLong();
      return TimestampData.fromLocalDateTime(Instant.ofEpochMilli(millis)
          .atOffset(ZoneOffset.UTC)
          .toLocalDateTime());
    }

    @Override
    public long readLong() {
      return column.nextLong();
    }
  }

  private static class StringReader extends ParquetValueReaders.PrimitiveReader<StringData> {
    StringReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public StringData read(StringData ignored) {
      Binary binary = column.nextBinary();
      ByteBuffer buffer = binary.toByteBuffer();
      if (buffer.hasArray()) {
        return StringData.fromBytes(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
        return StringData.fromBytes(binary.getBytes());
      }
    }
  }

  private static class LossyMicrosToMillisTimeReader extends ParquetValueReaders.PrimitiveReader<Integer> {
    LossyMicrosToMillisTimeReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Integer read(Integer reuse) {
      // Discard microseconds since Flink uses millisecond unit for TIME type.
      return (int) Math.floorDiv(column.nextLong(), 1000);
    }
  }

  private static class MillisTimeReader extends ParquetValueReaders.PrimitiveReader<Integer> {
    MillisTimeReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Integer read(Integer reuse) {
      return (int) column.nextLong();
    }
  }

  private static class ArrayReader<E> extends ParquetValueReaders.RepeatedReader<ArrayData, FlinkReusableArrayData, E> {
    private int readPos = 0;
    private int writePos = 0;

    ArrayReader(int definitionLevel, int repetitionLevel, ParquetValueReader<E> reader) {
      super(definitionLevel, repetitionLevel, reader);
    }

    @Override
    protected FlinkReusableArrayData newListData(ArrayData reuse) {
      this.readPos = 0;
      this.writePos = 0;

      if (reuse instanceof FlinkReusableArrayData) {
        return (FlinkReusableArrayData) reuse;
      } else {
        return new FlinkReusableArrayData();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected E getElement(FlinkReusableArrayData list) {
      E value = null;
      if (readPos < list.capacity()) {
        value = (E) list.getObj(readPos);
      }

      readPos += 1;

      return value;
    }

    @Override
    protected void addElement(FlinkReusableArrayData reused, E element) {
      if (writePos >= reused.capacity()) {
        reused.grow();
      }

      reused.update(writePos, element);

      writePos += 1;
    }

    @Override
    protected ArrayData buildList(FlinkReusableArrayData list) {
      list.setNumElements(writePos);
      return list;
    }
  }

  private static class MapReader<K, V> extends
      ParquetValueReaders.RepeatedKeyValueReader<MapData, FlinkReusableMapData, K, V> {
    private int readPos = 0;

    private final ParquetValueReaders.ReusableEntry<K, V> entry = new ParquetValueReaders.ReusableEntry<>();
    private final ParquetValueReaders.ReusableEntry<K, V> nullEntry = new ParquetValueReaders.ReusableEntry<>();

    MapReader(int definitionLevel, int repetitionLevel,
              ParquetValueReader<K> keyReader, ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
    }

    @Override
    protected FlinkReusableMapData newMapData(MapData reuse) {
      this.readPos = 0;

      if (reuse instanceof FlinkReusableMapData) {
        return (FlinkReusableMapData) reuse;
      } else {
        return new FlinkReusableMapData();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map.Entry<K, V> getPair(FlinkReusableMapData map) {
      Map.Entry<K, V> kv = map.getRaw(readPos, entry, nullEntry);
      readPos += 1;

      return kv;
    }

    @Override
    protected void addPair(FlinkReusableMapData map, K key, V value) {
      map.addPair(key, value);
    }

    @Override
    protected MapData buildMap(FlinkReusableMapData map) {
      return map;
    }
  }

  private static class FlinkReusableMapData implements ReusableMapData, MapData {

    private ReusableArrayData keys = null;
    private ReusableArrayData values = null;

    private FlinkReusableMapData() {
      this.keys = new FlinkReusableArrayData();
      this.values = new FlinkReusableArrayData();
    }

    @Override
    public FlinkReusableArrayData values() {
      return (FlinkReusableArrayData) values;
    }

    @Override
    public FlinkReusableArrayData keys() {
      return (FlinkReusableArrayData) keys;
    }

    @Override
    public FlinkReusableArrayData valueArray() {
      return (FlinkReusableArrayData) values;
    }

    @Override
    public FlinkReusableArrayData keyArray() {
      return (FlinkReusableArrayData) keys;
    }

    @Override
    public int size() {
      return keys.getNumElements();
    }
  }

  private static class FlinkReusableArrayData implements ReusableArrayData, ArrayData {

    private Object[] values = EMPTY;
    private int numElements = 0;

    @Override
    public Object[] values() {
      return values;
    }

    @Override
    public void setValues(Object[] array) {
      values = array;
    }

    @Override
    public void setNumElements(int num) {
      numElements = num;
    }

    @Override
    public int getNumElements() {
      return numElements;
    }

    @Override
    public int size() {
      return numElements;
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return null == values[ordinal];
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return (boolean) values[ordinal];
    }

    @Override
    public byte getByte(int ordinal) {
      return (byte) values[ordinal];
    }

    @Override
    public short getShort(int ordinal) {
      return (short) values[ordinal];
    }

    @Override
    public int getInt(int ordinal) {
      return (int) values[ordinal];
    }

    @Override
    public long getLong(int ordinal) {
      return (long) values[ordinal];
    }

    @Override
    public float getFloat(int ordinal) {
      return (float) values[ordinal];
    }

    @Override
    public double getDouble(int ordinal) {
      return (double) values[ordinal];
    }

    @Override
    public Object getObj(int ordinal) {
      return values[ordinal];
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return (byte[]) values[ordinal];
    }

    @Override
    public StringData getString(int pos) {
      return (StringData) values[pos];
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
      return (DecimalData) values[pos];
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
      return (TimestampData) values[pos];
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
      return (RawValueData<T>) values[pos];
    }

    @Override
    public ArrayData getArray(int ordinal) {
      return (ArrayData) values[ordinal];
    }

    @Override
    public MapData getMap(int ordinal) {
      return (MapData) values[ordinal];
    }

    @Override
    public RowData getRow(int pos, int numFields) {
      return (RowData) values[pos];
    }

    @Override
    public boolean[] toBooleanArray() {
      return ArrayUtils.toPrimitive((Boolean[]) values);
    }

    @Override
    public byte[] toByteArray() {
      return ArrayUtils.toPrimitive((Byte[]) values);
    }

    @Override
    public short[] toShortArray() {
      return ArrayUtils.toPrimitive((Short[]) values);
    }

    @Override
    public int[] toIntArray() {
      return ArrayUtils.toPrimitive((Integer[]) values);
    }

    @Override
    public long[] toLongArray() {
      return ArrayUtils.toPrimitive((Long[]) values);
    }

    @Override
    public float[] toFloatArray() {
      return ArrayUtils.toPrimitive((Float[]) values);
    }

    @Override
    public double[] toDoubleArray() {
      return ArrayUtils.toPrimitive((Double[]) values);
    }
  }

  private static class RowDataReader extends ParquetValueReaders.StructReader<RowData, GenericRowData> {
    private final int numFields;

    RowDataReader(List<Type> types, List<ParquetValueReader<?>> readers) {
      super(types, readers);
      this.numFields = readers.size();
    }

    @Override
    protected GenericRowData newStructData(RowData reuse) {
      if (reuse instanceof GenericRowData) {
        return (GenericRowData) reuse;
      } else {
        return new GenericRowData(numFields);
      }
    }

    @Override
    protected Object getField(GenericRowData intermediate, int pos) {
      return intermediate.getField(pos);
    }

    @Override
    protected RowData buildStruct(GenericRowData struct) {
      return struct;
    }

    @Override
    protected void set(GenericRowData row, int pos, Object value) {
      row.setField(pos, value);
    }

    @Override
    protected void setNull(GenericRowData row, int pos) {
      row.setField(pos, null);
    }

    @Override
    protected void setBoolean(GenericRowData row, int pos, boolean value) {
      row.setField(pos, value);
    }

    @Override
    protected void setInteger(GenericRowData row, int pos, int value) {
      row.setField(pos, value);
    }

    @Override
    protected void setLong(GenericRowData row, int pos, long value) {
      row.setField(pos, value);
    }

    @Override
    protected void setFloat(GenericRowData row, int pos, float value) {
      row.setField(pos, value);
    }

    @Override
    protected void setDouble(GenericRowData row, int pos, double value) {
      row.setField(pos, value);
    }
  }
}
