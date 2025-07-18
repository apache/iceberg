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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DataWriteBuilder;
import org.apache.iceberg.data.EqualityDeleteWriteBuilder;
import org.apache.iceberg.data.FormatModelRegistry;
import org.apache.iceberg.data.PositionDeleteWriteBuilder;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.data.FlinkFormatModels;
import org.apache.iceberg.flink.data.FlinkSchemaVisitor;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteBuilder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class FlinkAppenderFactory implements FileAppenderFactory<RowData>, Serializable {
  private final Schema schema;
  private final RowType flinkSchema;
  private final Map<String, String> props;
  private final PartitionSpec spec;
  private final int[] equalityFieldIds;
  private final Schema eqDeleteRowSchema;
  private final Schema posDeleteRowSchema;
  private final Table table;

  public FlinkAppenderFactory(
      Table table,
      Schema schema,
      RowType flinkSchema,
      Map<String, String> props,
      PartitionSpec spec,
      int[] equalityFieldIds,
      Schema eqDeleteRowSchema,
      Schema posDeleteRowSchema) {
    Preconditions.checkNotNull(table, "Table shouldn't be null");
    this.table = table;
    this.schema = schema;
    this.flinkSchema = flinkSchema;
    this.props = props;
    this.spec = spec;
    this.equalityFieldIds = equalityFieldIds;
    this.eqDeleteRowSchema = eqDeleteRowSchema;
    this.posDeleteRowSchema = posDeleteRowSchema;
  }

  public RowType fs() {
    return flinkSchema;
  }

  @Override
  public FileAppender<RowData> newAppender(OutputFile outputFile, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      WriteBuilder<?, RowData> builder =
          FormatModelRegistry.writeBuilder(
              format,
              FlinkFormatModels.MODEL_NAME,
              EncryptedFiles.plainAsEncryptedOutput(outputFile));
      return new WrappedFileAppender(builder
          .set(props)
          .fileSchema(schema)
          .metricsConfig(metricsConfig)
          .overwrite()
          .build(), flinkSchema, schema.asStruct());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DataWriter<RowData> newDataWriter(
      EncryptedOutputFile file, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      DataWriteBuilder<?, RowData> builder =
          FormatModelRegistry.dataWriteBuilder(format, FlinkFormatModels.MODEL_NAME, file);
      return new WrappedDataWriter(builder
          .set(props)
          .fileSchema(schema)
          .metricsConfig(metricsConfig)
          .overwrite()
          .spec(spec)
          .partition(partition)
          .keyMetadata(file.keyMetadata())
          .build(), flinkSchema, schema.asStruct());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public EqualityDeleteWriter<RowData> newEqDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    Preconditions.checkState(
        equalityFieldIds != null && equalityFieldIds.length > 0,
        "Equality field ids shouldn't be null or empty when creating equality-delete writer");
    Preconditions.checkNotNull(
        eqDeleteRowSchema,
        "Equality delete row schema shouldn't be null when creating equality-delete writer");

    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    try {
      EqualityDeleteWriteBuilder<?, RowData> builder =
          FormatModelRegistry.equalityDeleteWriteBuilder(
              format, FlinkFormatModels.MODEL_NAME, outputFile);
      return builder
          .overwrite()
          .set(props)
          .metricsConfig(metricsConfig)
          .partition(partition)
          .rowSchema(eqDeleteRowSchema)
          .spec(spec)
          .keyMetadata(outputFile.keyMetadata())
          .equalityFieldIds(equalityFieldIds)
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public PositionDeleteWriter<RowData> newPosDeleteWriter(
      EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    MetricsConfig metricsConfig = MetricsConfig.forPositionDelete(table);
    try {
      PositionDeleteWriteBuilder<?, RowData> builder =
          FormatModelRegistry.positionDeleteWriteBuilder(
              format, FlinkFormatModels.MODEL_NAME, outputFile);
      return builder
          .overwrite()
          .set(props)
          .metricsConfig(metricsConfig)
          .partition(partition)
          .rowSchema(posDeleteRowSchema)
          .spec(spec)
          .keyMetadata(outputFile.keyMetadata())
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class WrappedDataWriter extends DataWriter<RowData> {
    private final DataWriter<RowData> delegate;
    private final RowDataTransformer transformer;

    public WrappedDataWriter(DataWriter<RowData> delegate, RowType rowType, Types.StructType struct) {
      super(delegate);
      this.delegate = delegate;
      this.transformer = new RowDataTransformer(rowType, struct);
    }

    @Override
    public void write(RowData row) {
      delegate.write(transformer.wrap(row));
    }
  }

  private static class WrappedFileAppender implements FileAppender<RowData> {
    private final FileAppender<RowData> delegate;
    private final RowDataTransformer transformer;

    public WrappedFileAppender(FileAppender<RowData> delegate, RowType rowType, Types.StructType struct) {
      this.delegate = delegate;
      this.transformer = new RowDataTransformer(rowType, struct);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public void add(RowData datum) {
      delegate.add(transformer.wrap(datum));
    }

    @Override
    public Metrics metrics() {
      return delegate.metrics();
    }

    @Override
    public long length() {
      return delegate.length();
    }

    @Override
    public List<Long> splitOffsets() {
      return delegate.splitOffsets();
    }
  }
//
//  private interface PositionalGetter<T, I> {
//    T get(I data, int pos);
//  }
//
//  private static class RowDataWrapper implements RowData {
//
//    private final LogicalType[] types;
//    private final PositionalGetter<?, RowData>[] getters;
//    private RowData rowData = null;
//
//    public RowDataWrapper(RowType rowType, Types.StructType struct) {
//      int size = rowType.getFieldCount();
//
//      types = (LogicalType[]) Array.newInstance(LogicalType.class, size);
//      getters = (PositionalGetter<?, RowData>[]) Array.newInstance(PositionalGetter.class, size);
//
//      for (int i = 0; i < size; i++) {
//        types[i] = rowType.getTypeAt(i);
//        getters[i] = buildGetter(types[i], struct.fields().get(i).type());
//      }
//    }
//
//    public RowData wrap(RowData data) {
//      this.rowData = data;
//      return this;
//    }
//
//    @Override
//    public int getArity() {
//      return rowData.getArity();
//    }
//
//    @Override
//    public RowKind getRowKind() {
//      return rowData.getRowKind();
//    }
//
//    @Override
//    public void setRowKind(RowKind kind) {
//      throw new UnsupportedOperationException("Not supported in RowDataWrapper");
//    }
//
//    @Override
//    public boolean isNullAt(int pos) {
//      return rowData.isNullAt(pos);
//    }
//
//    @Override
//    public boolean getBoolean(int pos) {
//      return (boolean) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public byte getByte(int pos) {
//      return (byte) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public short getShort(int pos) {
//      return (short) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public int getInt(int pos) {
//      return (int) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public long getLong(int pos) {
//      return (long) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public float getFloat(int pos) {
//      return (float) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public double getDouble(int pos) {
//      return (double) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public StringData getString(int pos) {
//      return (StringData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public DecimalData getDecimal(int pos, int precision, int scale) {
//      return (DecimalData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public TimestampData getTimestamp(int pos, int precision) {
//      return (TimestampData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public <T> RawValueData<T> getRawValue(int pos) {
//      return (RawValueData<T>) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public byte[] getBinary(int pos) {
//      return (byte[]) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public ArrayData getArray(int pos) {
//      return (ArrayData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public MapData getMap(int pos) {
//      return (MapData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public RowData getRow(int pos, int numFields) {
//      return (RowData) getters[pos].get(rowData, pos);
//    }
//
//    private PositionalGetter<?, RowData> buildGetter(LogicalType logicalType, Type type) {
//      switch (type.typeId()) {
//        case BOOLEAN:
//          return RowData::getBoolean;
//        case INTEGER:
//          if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
//            return (row, pos) -> (int) row.getByte(pos);
//          } else if (logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
//            return (row, pos) -> (int) row.getShort(pos);
//          }
//          return RowData::getInt;
//        case LONG:
//          return RowData::getLong;
//        case FLOAT:
//          return RowData::getFloat;
//        case DOUBLE:
//          return RowData::getDouble;
//        case STRING:
//          return RowData::getString;
//        case DECIMAL:
//          DecimalType decimalType = (DecimalType) logicalType;
//          return (row, pos) ->
//              row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
//        case DATE:
//        case TIME:
//          return RowData::getInt;
//        case TIMESTAMP:
//          TimestampType timestampType = (TimestampType) logicalType;
//          return (row, pos) -> row.getTimestamp(pos, timestampType.getPrecision());
//        case TIMESTAMP_NANO:
//          TimestampType nanoTimestampType = (TimestampType) logicalType;
//          return (row, pos) ->
//              row.getTimestamp(pos, nanoTimestampType.getPrecision());
//        case LIST:
//            ArrayType arrayType = (ArrayType) logicalType;
//          Types.ListType listType = type.asListType();
//            return (row, pos) -> {
//              ArrayData arrayData = row.getArray(pos);
//              PositionalGetter internalGetter =
//                  buildGetter(arrayType.getElementType(), listType.elementType());
//                if (arrayData == null) {
//                    return null;
//                }
//              if (arrayType == null) {
//                return null;
//              }
//              if (listType == null) {
//                return null;
//              }
//              return new GenericArrayData(new byte[] {(byte) 0x04, (byte) 0x05});
//            };
//
//        default:
//          throw new UnsupportedOperationException("Not supported type: " + type.typeId());
//      }
////      switch (logicalType.getTypeRoot()) {
////        case TINYINT:
////          return (row, pos) -> (int) row.getByte(pos);
////        case SMALLINT:
////          return (row, pos) -> (int) row.getShort(pos);
////        case CHAR:
////        case VARCHAR:
////          return (row, pos) -> row.getString(pos).toString();
////        case BINARY:
////        case VARBINARY:
////          if (Type.TypeID.UUID == type.typeId()) {
////            return (row, pos) -> UUIDUtil.convert(row.getBinary(pos));
////          } else {
////            return (row, pos) -> ByteBuffer.wrap(row.getBinary(pos));
////          }
////
////        case DECIMAL:
////          DecimalType decimalType = (DecimalType) logicalType;
////          return (row, pos) ->
////                  row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale()).toBigDecimal();
////
////        case TIME_WITHOUT_TIME_ZONE:
////          // Time in RowData is in milliseconds (Integer), while iceberg's time is microseconds
////          // (Long).
////          return (row, pos) -> ((long) row.getInt(pos)) * 1_000;
////
////        case TIMESTAMP_WITHOUT_TIME_ZONE:
////          TimestampType timestampType = (TimestampType) logicalType;
////          return (row, pos) -> {
////            LocalDateTime localDateTime =
////                    row.getTimestamp(pos, timestampType.getPrecision()).toLocalDateTime();
////            return DateTimeUtil.microsFromTimestamp(localDateTime);
////          };
////
////        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
////          LocalZonedTimestampType lzTs = (LocalZonedTimestampType) logicalType;
////          return (row, pos) -> {
////            TimestampData timestampData = row.getTimestamp(pos, lzTs.getPrecision());
////            return timestampData.getMillisecond() * 1000
////                    + timestampData.getNanoOfMillisecond() / 1000;
////          };
////
////        case ROW:
////          RowType rowType = (RowType) logicalType;
////          Types.StructType structType = (Types.StructType) type;
////
////          RowDataWrapper nestedWrapper = new RowDataWrapper(rowType, structType);
////          return (row, pos) -> nestedWrapper.wrap(row.getRow(pos, rowType.getFieldCount()));
////
////        default:
////          return null;
////      }
//    }
//  }
//
//  private static class RowDataAccessor implements RowData {
//    private final PositionalGetter<?, RowData>[] getters;
//    private RowData rowData = null;
//
//    public RowDataAccessor(PositionalGetter<?, RowData>[] getters) {
//      this.getters = getters;
//    }
//
//    public RowData wrap(RowData data) {
//      this.rowData = data;
//      return this;
//    }
//
//    @Override
//    public int getArity() {
//      return rowData.getArity();
//    }
//
//    @Override
//    public RowKind getRowKind() {
//      return rowData.getRowKind();
//    }
//
//    @Override
//    public void setRowKind(RowKind kind) {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public boolean isNullAt(int pos) {
//      return rowData.isNullAt(pos);
//    }
//
//    @Override
//    public boolean getBoolean(int pos) {
//      return (boolean) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public byte getByte(int pos) {
//      return (byte) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public short getShort(int pos) {
//      return (short) getters[pos].get(rowData, pos);
//
//    }
//
//    @Override
//    public int getInt(int pos) {
//      return (int) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public long getLong(int pos) {
//      return (long) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public float getFloat(int pos) {
//      return (float) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public double getDouble(int pos) {
//      return (double) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public StringData getString(int pos) {
//      return (StringData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public DecimalData getDecimal(int pos, int precision, int scale) {
//      return (DecimalData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public TimestampData getTimestamp(int pos, int precision) {
//      return (TimestampData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public <T> RawValueData<T> getRawValue(int pos) {
//      return (RawValueData<T>) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public byte[] getBinary(int pos) {
//      return (byte[]) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public ArrayData getArray(int pos) {
//      return (ArrayData) getters[pos].get(rowData, pos);
//    }
//
//    @Override
//    public MapData getMap(int pos) {
//      return (MapData) getters[pos].get(rowData, pos);
//
//    }
//
//    @Override
//    public RowData getRow(int pos, int numFields) {
//      return (RowData) getters[pos].get(rowData, pos);
//    }
//  }
//
//  private static class ArrayDataAccessor implements ArrayData {
//    private final PositionalGetter<?, ArrayData> getter;
//    private ArrayData arrayData = null;
//
//    public ArrayDataAccessor(PositionalGetter<?, ArrayData> getter) {
//      this.getter = getter;
//    }
//
//    public ArrayData wrap(ArrayData data) {
//      this.arrayData = data;
//      return this;
//    }
//
//    @Override
//    public int size() {
//      return arrayData.size();
//    }
//
//    @Override
//    public boolean[] toBooleanArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public byte[] toByteArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public short[] toShortArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public int[] toIntArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public long[] toLongArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public float[] toFloatArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public double[] toDoubleArray() {
//      throw new UnsupportedOperationException("Not supported in RowDataAccessor");
//    }
//
//    @Override
//    public boolean isNullAt(int pos) {
//      return arrayData.isNullAt(pos);
//    }
//
//    @Override
//    public boolean getBoolean(int pos) {
//      return (boolean) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public byte getByte(int pos) {
//      return (byte) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public short getShort(int pos) {
//      return (short) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public int getInt(int pos) {
//      return (int) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public long getLong(int pos) {
//      return (long) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public float getFloat(int pos) {
//      return (float) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public double getDouble(int pos) {
//      return (double) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public StringData getString(int pos) {
//      return (StringData) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public DecimalData getDecimal(int pos, int precision, int scale) {
//      return (DecimalData) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public TimestampData getTimestamp(int pos, int precision) {
//      return (TimestampData) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public <T> RawValueData<T> getRawValue(int pos) {
//      return (RawValueData<T>) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public byte[] getBinary(int pos) {
//      return (byte[]) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public ArrayData getArray(int pos) {
//      return (ArrayData) getter.get(arrayData, pos);
//    }
//
//    @Override
//    public MapData getMap(int pos) {
//      return (MapData) getter.get(arrayData, pos);
//
//    }
//
//    @Override
//    public RowData getRow(int pos, int numFields) {
//      return (RowData) getter.get(arrayData, pos);
//    }
//  }
//
//  private static class MapDataAccessor implements MapData {
//    private final ArrayDataAccessor keyAccessor;
//    private final ArrayDataAccessor valueAccessor;
//    private ArrayData keyData = null;
//    private ArrayData valueData = null;
//
//    public MapDataAccessor(PositionalGetter<?, ArrayData> keyGetter,
//                           PositionalGetter<?, ArrayData> valueGetter) {
//      this.keyAccessor = new ArrayDataAccessor(keyGetter);
//      this.valueAccessor = new ArrayDataAccessor(valueGetter);
//    }
//
//    public MapData wrap(ArrayData keyData, ArrayData valueData) {
//      this.keyData = keyAccessor.wrap(keyData);
//      this.valueData = valueAccessor.wrap(valueData);
//      return this;
//    }
//
//    @Override
//    public int size() {
//      return keyData.size();
//    }
//
//    @Override
//    public ArrayData keyArray() {
//      return keyAccessor.wrap(keyData);
//    }
//
//    @Override
//    public ArrayData valueArray() {
//      return valueAccessor.wrap(valueData);
//    }
//  }
//
//  private static class RowDataVisitor extends FlinkSchemaVisitor<PositionalGetter<?, RowData>> {
//    @Override
//    public PositionalGetter<?, RowData> record(Types.StructType iStruct, List<PositionalGetter<?, RowData>> results, List<LogicalType> fieldTypes) {
//      RowDataAccessor accessor = new RowDataAccessor(results.toArray(new PositionalGetter[0]));
//      return ((data, pos) -> {
//        RowData row = data.getRow(pos, iStruct.fields().size());
//        return accessor.wrap(row);
//      });
//    }
//
//    public PositionalGetter<?, RowData> list(Types.ListType iList, PositionalGetter<?, RowData> getter, LogicalType elementType) {
//      ArrayDataAccessor accessor = new ArrayDataAccessor(arrayPrimitive(iList.elementType(), getter, elementType));
//      return (data, pos) -> {
//        ArrayData arrayData = data.getArray(pos);
//        return accessor.wrap(arrayData);
//      };
//    }
//
//    public PositionalGetter<?, RowData> map(Types.MapType iMap, PositionalGetter<?, RowData> keyGetter, PositionalGetter<?, RowData> valueGetter, LogicalType keyType, LogicalType valueType) {
//      MapDataAccessor accessor = new MapDataAccessor(arrayPrimitive(iMap.keyType(), keyGetter, keyType), arrayPrimitive(iMap.valueType(), valueGetter, valueType));
//      return (data, pos) -> {
//        MapData mapData = data.getMap(pos);
//        ArrayData keyData = mapData.keyArray();
//        ArrayData valueData = mapData.valueArray();
//        return accessor.wrap(keyData, valueData);
//      };
//    }
//
//    public PositionalGetter<?, RowData> primitive(Type.PrimitiveType type, LogicalType logicalType) {
//      switch (type.typeId()) {
//        case BOOLEAN:
//          return RowData::getBoolean;
//        case INTEGER:
//          if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
//            return (row, pos) -> (int) row.getByte(pos);
//          } else if (logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
//            return (row, pos) -> (int) row.getShort(pos);
//          }
//          return RowData::getInt;
//        case LONG:
//          return RowData::getLong;
//        case FLOAT:
//          return RowData::getFloat;
//        case DOUBLE:
//          return RowData::getDouble;
//        case STRING:
//          return RowData::getString;
//        case DECIMAL:
//          DecimalType decimalType = (DecimalType) logicalType;
//          return (row, pos) ->
//                  row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
//        case DATE:
//        case TIME:
//          return (row, pos) -> row.getInt(pos);
//        case TIMESTAMP:
//          TimestampType timestampType = (TimestampType) logicalType;
//          return (row, pos) -> row.getTimestamp(pos, timestampType.getPrecision());
//        case TIMESTAMP_NANO:
//          TimestampType nanoTimestampType = (TimestampType) logicalType;
//          return (row, pos) ->
//                  row.getTimestamp(pos, nanoTimestampType.getPrecision());
//        default:
//          throw new UnsupportedOperationException("Not supported type: " + type.typeId());
//      }
//    }
//
//    private PositionalGetter<?, ArrayData> arrayPrimitive(Type type, PositionalGetter<?, RowData> getter, LogicalType logicalType) {
//      switch (type.typeId()) {
//        case BOOLEAN:
//          return ArrayData::getBoolean;
//        case INTEGER:
//          if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
//            return (row, pos) -> (int) row.getByte(pos);
//          } else if (logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
//            return (row, pos) -> (int) row.getShort(pos);
//          }
//          return ArrayData::getInt;
//        case LONG:
//          return ArrayData::getLong;
//        case FLOAT:
//          return ArrayData::getFloat;
//        case DOUBLE:
//          return ArrayData::getDouble;
//        case STRING:
//          return ArrayData::getString;
//        case DECIMAL:
//          DecimalType decimalType = (DecimalType) logicalType;
//          return (row, pos) ->
//                  row.getDecimal(pos, decimalType.getPrecision(), decimalType.getScale());
//        case DATE:
//        case TIME:
//          return (row, pos) -> row.getInt(pos);
//        case TIMESTAMP:
//          TimestampType timestampType = (TimestampType) logicalType;
//          return (row, pos) -> row.getTimestamp(pos, timestampType.getPrecision());
//        case TIMESTAMP_NANO:
//          TimestampType nanoTimestampType = (TimestampType) logicalType;
//          return (row, pos) ->
//                  row.getTimestamp(pos, nanoTimestampType.getPrecision());
//        case LIST:
//            Types.ListType listType = type.asListType();
//            ArrayType arrayType = (ArrayType) logicalType;
//          ArrayDataAccessor arrayDataAccessor = new ArrayDataAccessor(arrayPrimitive(listType.elementType(), getter, arrayType.getElementType()));
//            return (row, pos) -> {
//                ArrayData arrayData = row.getArray(pos);
//                return arrayDataAccessor.wrap(arrayData);
//            };
//        default:
//          throw new UnsupportedOperationException("Not supported type: " + type.typeId());
//      }
//    }
//  }
}
