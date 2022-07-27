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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;

public class GenericOrcReaders {

  private GenericOrcReaders() {}

  public static OrcValueReader<Record> struct(
      List<OrcValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  public static OrcValueReader<List<?>> array(OrcValueReader<?> elementReader) {
    return new ListReader(elementReader);
  }

  public static OrcValueReader<Map<?, ?>> map(
      OrcValueReader<?> keyReader, OrcValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  public static OrcValueReader<OffsetDateTime> timestampTzs() {
    return TimestampTzReader.INSTANCE;
  }

  public static OrcValueReader<BigDecimal> decimals() {
    return DecimalReader.INSTANCE;
  }

  public static OrcValueReader<String> strings() {
    return StringReader.INSTANCE;
  }

  public static OrcValueReader<UUID> uuids() {
    return UUIDReader.INSTANCE;
  }

  public static OrcValueReader<ByteBuffer> bytes() {
    return BytesReader.INSTANCE;
  }

  public static OrcValueReader<LocalTime> times() {
    return TimeReader.INSTANCE;
  }

  public static OrcValueReader<LocalDate> dates() {
    return DateReader.INSTANCE;
  }

  public static OrcValueReader<LocalDateTime> timestamps() {
    return TimestampReader.INSTANCE;
  }

  private static class TimestampTzReader implements OrcValueReader<OffsetDateTime> {
    public static final OrcValueReader<OffsetDateTime> INSTANCE = new TimestampTzReader();

    private TimestampTzReader() {}

    @Override
    public OffsetDateTime nonNullRead(ColumnVector vector, int row) {
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      return Instant.ofEpochSecond(Math.floorDiv(tcv.time[row], 1_000), tcv.nanos[row])
          .atOffset(ZoneOffset.UTC);
    }
  }

  private static class TimeReader implements OrcValueReader<LocalTime> {
    public static final OrcValueReader<LocalTime> INSTANCE = new TimeReader();

    private TimeReader() {}

    @Override
    public LocalTime nonNullRead(ColumnVector vector, int row) {
      return DateTimeUtil.timeFromMicros(((LongColumnVector) vector).vector[row]);
    }
  }

  private static class DateReader implements OrcValueReader<LocalDate> {
    public static final OrcValueReader<LocalDate> INSTANCE = new DateReader();

    private DateReader() {}

    @Override
    public LocalDate nonNullRead(ColumnVector vector, int row) {
      return DateTimeUtil.dateFromDays((int) ((LongColumnVector) vector).vector[row]);
    }
  }

  private static class TimestampReader implements OrcValueReader<LocalDateTime> {
    public static final OrcValueReader<LocalDateTime> INSTANCE = new TimestampReader();

    private TimestampReader() {}

    @Override
    public LocalDateTime nonNullRead(ColumnVector vector, int row) {
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      return Instant.ofEpochSecond(Math.floorDiv(tcv.time[row], 1_000), tcv.nanos[row])
          .atOffset(ZoneOffset.UTC)
          .toLocalDateTime();
    }
  }

  private static class DecimalReader implements OrcValueReader<BigDecimal> {
    public static final OrcValueReader<BigDecimal> INSTANCE = new DecimalReader();

    private DecimalReader() {}

    @Override
    public BigDecimal nonNullRead(ColumnVector vector, int row) {
      DecimalColumnVector cv = (DecimalColumnVector) vector;
      return cv.vector[row].getHiveDecimal().bigDecimalValue().setScale(cv.scale);
    }
  }

  private static class StringReader implements OrcValueReader<String> {
    public static final OrcValueReader<String> INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public String nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      return new String(
          bytesVector.vector[row],
          bytesVector.start[row],
          bytesVector.length[row],
          StandardCharsets.UTF_8);
    }
  }

  private static class UUIDReader implements OrcValueReader<UUID> {
    public static final OrcValueReader<UUID> INSTANCE = new UUIDReader();

    private UUIDReader() {}

    @Override
    public UUID nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      ByteBuffer buf =
          ByteBuffer.wrap(bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row]);
      return UUIDUtil.convert(buf);
    }
  }

  private static class BytesReader implements OrcValueReader<ByteBuffer> {
    public static final OrcValueReader<ByteBuffer> INSTANCE = new BytesReader();

    private BytesReader() {}

    @Override
    public ByteBuffer nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      return ByteBuffer.wrap(
          bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row]);
    }
  }

  private static class StructReader extends OrcValueReaders.StructReader<Record> {
    private final GenericRecord template;

    protected StructReader(
        List<OrcValueReader<?>> readers,
        Types.StructType structType,
        Map<Integer, ?> idToConstant) {
      super(readers, structType, idToConstant);
      this.template = structType != null ? GenericRecord.create(structType) : null;
    }

    @Override
    protected Record create() {
      // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since
      // NAME_MAP_CACHE access
      // is eliminated. Using copy here to gain performance.
      return template.copy();
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }

  private static class MapReader implements OrcValueReader<Map<?, ?>> {
    private final OrcValueReader<?> keyReader;
    private final OrcValueReader<?> valueReader;

    private MapReader(OrcValueReader<?> keyReader, OrcValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public Map<?, ?> nonNullRead(ColumnVector vector, int row) {
      MapColumnVector mapVector = (MapColumnVector) vector;
      int offset = (int) mapVector.offsets[row];
      long length = mapVector.lengths[row];
      Map<Object, Object> map = Maps.newHashMapWithExpectedSize((int) length);
      for (int c = 0; c < length; c++) {
        map.put(
            keyReader.read(mapVector.keys, offset + c),
            valueReader.read(mapVector.values, offset + c));
      }
      return map;
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      keyReader.setBatchContext(batchOffsetInFile);
      valueReader.setBatchContext(batchOffsetInFile);
    }
  }

  private static class ListReader implements OrcValueReader<List<?>> {
    private final OrcValueReader<?> elementReader;

    private ListReader(OrcValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public List<?> nonNullRead(ColumnVector vector, int row) {
      ListColumnVector listVector = (ListColumnVector) vector;
      int offset = (int) listVector.offsets[row];
      int length = (int) listVector.lengths[row];
      List<Object> elements = Lists.newArrayListWithExpectedSize(length);
      for (int c = 0; c < length; ++c) {
        elements.add(elementReader.read(listVector.child, offset + c));
      }
      return elements;
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      elementReader.setBatchContext(batchOffsetInFile);
    }
  }
}
