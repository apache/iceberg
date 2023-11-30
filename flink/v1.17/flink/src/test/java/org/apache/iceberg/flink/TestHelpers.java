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
package org.apache.iceberg.flink;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkInputSplit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.assertj.core.api.Assertions;

public class TestHelpers {
  private TestHelpers() {}

  public static <T> T roundTripKryoSerialize(Class<T> clazz, T table) throws IOException {
    KryoSerializer<T> kryo = new KryoSerializer<>(clazz, new ExecutionConfig());

    DataOutputSerializer outputView = new DataOutputSerializer(1024);
    kryo.serialize(table, outputView);

    DataInputDeserializer inputView = new DataInputDeserializer(outputView.getCopyOfBuffer());
    return kryo.deserialize(inputView);
  }

  public static RowData copyRowData(RowData from, RowType rowType) {
    TypeSerializer[] fieldSerializers =
        rowType.getChildren().stream()
            .map((LogicalType type) -> InternalSerializers.create(type))
            .toArray(TypeSerializer[]::new);
    return RowDataUtil.clone(from, null, rowType, fieldSerializers);
  }

  public static void readRowData(FlinkInputFormat input, Consumer<RowData> visitor)
      throws IOException {
    for (FlinkInputSplit s : input.createInputSplits(0)) {
      input.open(s);
      try {
        while (!input.reachedEnd()) {
          RowData row = input.nextRecord(null);
          visitor.accept(row);
        }
      } finally {
        input.close();
      }
    }
  }

  public static List<RowData> readRowData(FlinkInputFormat inputFormat, RowType rowType)
      throws IOException {
    List<RowData> results = Lists.newArrayList();
    readRowData(inputFormat, row -> results.add(copyRowData(row, rowType)));
    return results;
  }

  public static List<Row> readRows(FlinkInputFormat inputFormat, RowType rowType)
      throws IOException {
    return convertRowDataToRow(readRowData(inputFormat, rowType), rowType);
  }

  public static List<Row> convertRowDataToRow(List<RowData> rowDataList, RowType rowType) {
    DataStructureConverter<Object, Object> converter =
        DataStructureConverters.getConverter(TypeConversions.fromLogicalToDataType(rowType));
    return rowDataList.stream()
        .map(converter::toExternal)
        .map(Row.class::cast)
        .collect(Collectors.toList());
  }

  public static void assertRecords(List<Row> results, List<Record> expectedRecords, Schema schema) {
    List<Row> expected = Lists.newArrayList();
    @SuppressWarnings("unchecked")
    DataStructureConverter<RowData, Row> converter =
        (DataStructureConverter)
            DataStructureConverters.getConverter(
                TypeConversions.fromLogicalToDataType(FlinkSchemaUtil.convert(schema)));
    expectedRecords.forEach(
        r -> expected.add(converter.toExternal(RowDataConverter.convert(schema, r))));
    assertRows(results, expected);
  }

  public static void assertRows(List<RowData> results, List<RowData> expected, RowType rowType) {
    assertRows(convertRowDataToRow(results, rowType), convertRowDataToRow(expected, rowType));
  }

  public static void assertRows(List<Row> results, List<Row> expected) {
    Assertions.assertThat(results).containsExactlyInAnyOrderElementsOf(expected);
  }

  public static void assertRowData(Schema schema, StructLike expected, RowData actual) {
    assertRowData(schema.asStruct(), FlinkSchemaUtil.convert(schema), expected, actual);
  }

  public static void assertRowData(
      Types.StructType structType,
      LogicalType rowType,
      StructLike expectedRecord,
      RowData actualRowData) {
    if (expectedRecord == null && actualRowData == null) {
      return;
    }

    Assertions.assertThat(expectedRecord != null && actualRowData != null)
        .as("expected Record and actual RowData should be both null or not null")
        .isTrue();

    List<Type> types = Lists.newArrayList();
    for (Types.NestedField field : structType.fields()) {
      types.add(field.type());
    }

    for (int i = 0; i < types.size(); i += 1) {
      LogicalType logicalType = ((RowType) rowType).getTypeAt(i);
      Object expected = expectedRecord.get(i, Object.class);
      // The RowData.createFieldGetter won't return null for the required field. But in the
      // projection case, if we are
      // projecting a nested required field from an optional struct, then we should give a null for
      // the projected field
      // if the outer struct value is null. So we need to check the nullable for actualRowData here.
      // For more details
      // please see issue #2738.
      Object actual =
          actualRowData.isNullAt(i)
              ? null
              : RowData.createFieldGetter(logicalType, i).getFieldOrNull(actualRowData);
      assertEquals(types.get(i), logicalType, expected, actual);
    }
  }

  private static void assertEquals(
      Type type, LogicalType logicalType, Object expected, Object actual) {

    if (expected == null && actual == null) {
      return;
    }

    Assertions.assertThat(expected != null && actual != null)
        .as("expected and actual should be both null or not null")
        .isTrue();

    switch (type.typeId()) {
      case BOOLEAN:
        Assertions.assertThat(actual).as("boolean value should be equal").isEqualTo(expected);
        break;
      case INTEGER:
        Assertions.assertThat(actual).as("int value should be equal").isEqualTo(expected);
        break;
      case LONG:
        Assertions.assertThat(actual).as("long value should be equal").isEqualTo(expected);
        break;
      case FLOAT:
        Assertions.assertThat(actual).as("float value should be equal").isEqualTo(expected);
        break;
      case DOUBLE:
        Assertions.assertThat(actual).as("double value should be equal").isEqualTo(expected);
        break;
      case STRING:
        Assertions.assertThat(expected)
            .as("Should expect a CharSequence")
            .isInstanceOf(CharSequence.class);
        Assertions.assertThat(actual.toString())
            .as("string should be equal")
            .isEqualTo(String.valueOf(expected));
        break;
      case DATE:
        Assertions.assertThat(expected).as("Should expect a Date").isInstanceOf(LocalDate.class);
        LocalDate date = DateTimeUtil.dateFromDays((int) actual);
        Assertions.assertThat(date).as("date should be equal").isEqualTo(expected);
        break;
      case TIME:
        Assertions.assertThat(expected)
            .as("Should expect a LocalTime")
            .isInstanceOf(LocalTime.class);
        int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
        Assertions.assertThat(actual).as("time millis should be equal").isEqualTo(milliseconds);
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          Assertions.assertThat(expected)
              .as("Should expect a OffsetDataTime")
              .isInstanceOf(OffsetDateTime.class);
          OffsetDateTime ts = (OffsetDateTime) expected;
          Assertions.assertThat(((TimestampData) actual).toLocalDateTime())
              .as("OffsetDataTime should be equal")
              .isEqualTo(ts.toLocalDateTime());
        } else {
          Assertions.assertThat(expected)
              .as("Should expect a LocalDataTime")
              .isInstanceOf(LocalDateTime.class);
          LocalDateTime ts = (LocalDateTime) expected;
          Assertions.assertThat(((TimestampData) actual).toLocalDateTime())
              .as("LocalDataTime should be equal")
              .isEqualTo(ts);
        }
        break;
      case BINARY:
        Assertions.assertThat(expected)
            .as("Should expect a ByteBuffer")
            .isInstanceOf(ByteBuffer.class);
        Assertions.assertThat(ByteBuffer.wrap((byte[]) actual))
            .as("binary should be equal")
            .isEqualTo(expected);
        break;
      case DECIMAL:
        Assertions.assertThat(expected)
            .as("Should expect a BigDecimal")
            .isInstanceOf(BigDecimal.class);
        BigDecimal bd = (BigDecimal) expected;
        Assertions.assertThat(((DecimalData) actual).toBigDecimal())
            .as("decimal value should be equal")
            .isEqualTo(bd);
        break;
      case LIST:
        Assertions.assertThat(expected)
            .as("Should expect a Collection")
            .isInstanceOf(Collection.class);
        Collection<?> expectedArrayData = (Collection<?>) expected;
        ArrayData actualArrayData = (ArrayData) actual;
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        Assertions.assertThat(actualArrayData.size())
            .as("array length should be equal")
            .isEqualTo(expectedArrayData.size());
        assertArrayValues(
            type.asListType().elementType(), elementType, expectedArrayData, actualArrayData);
        break;
      case MAP:
        Assertions.assertThat(expected).as("Should expect a Map").isInstanceOf(Map.class);
        assertMapValues(type.asMapType(), logicalType, (Map<?, ?>) expected, (MapData) actual);
        break;
      case STRUCT:
        Assertions.assertThat(expected).as("Should expect a Record").isInstanceOf(StructLike.class);
        assertRowData(type.asStructType(), logicalType, (StructLike) expected, (RowData) actual);
        break;
      case UUID:
        Assertions.assertThat(expected).as("Should expect a UUID").isInstanceOf(UUID.class);
        ByteBuffer bb = ByteBuffer.wrap((byte[]) actual);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        Assertions.assertThat(new UUID(firstLong, secondLong).toString())
            .as("UUID should be equal")
            .isEqualTo(expected.toString());
        break;
      case FIXED:
        Assertions.assertThat(expected).as("Should expect byte[]").isInstanceOf(byte[].class);
        Assertions.assertThat((byte[]) actual).as("binary should be equal").isEqualTo(expected);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  public static void assertEquals(Schema schema, List<GenericData.Record> records, List<Row> rows) {
    Streams.forEachPair(
        records.stream(), rows.stream(), (record, row) -> assertEquals(schema, record, row));
  }

  public static void assertEquals(Schema schema, GenericData.Record record, Row row) {
    List<Types.NestedField> fields = schema.asStruct().fields();
    Assertions.assertThat(fields.size()).isEqualTo(record.getSchema().getFields().size());
    Assertions.assertThat(fields.size()).isEqualTo(row.getArity());
    RowType rowType = FlinkSchemaUtil.convert(schema);
    for (int i = 0; i < fields.size(); ++i) {
      Type fieldType = fields.get(i).type();
      Object expectedValue = record.get(i);
      Object actualValue = row.getField(i);
      LogicalType logicalType = rowType.getTypeAt(i);
      assertAvroEquals(fieldType, logicalType, expectedValue, actualValue);
    }
  }

  private static void assertEquals(Types.StructType struct, GenericData.Record record, Row row) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();
      Object expectedValue = record.get(i);
      Object actualValue = row.getField(i);
      assertAvroEquals(fieldType, null, expectedValue, actualValue);
    }
  }

  private static void assertAvroEquals(
      Type type, LogicalType logicalType, Object expected, Object actual) {

    if (expected == null && actual == null) {
      return;
    }

    Assertions.assertThat(expected != null && actual != null)
        .as("expected and actual should be both null or not null")
        .isTrue();

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
        Assertions.assertThat(expected)
            .as("Should expect a " + type.typeId().javaClass())
            .isInstanceOf(type.typeId().javaClass());
        Assertions.assertThat(actual)
            .as("Should expect a " + type.typeId().javaClass())
            .isInstanceOf(type.typeId().javaClass());
        Assertions.assertThat(actual)
            .as(type.typeId() + " value should be equal")
            .isEqualTo(expected);
        break;
      case STRING:
        Assertions.assertThat(expected)
            .as("Should expect a CharSequence")
            .isInstanceOf(CharSequence.class);
        Assertions.assertThat(actual)
            .as("Should expect a CharSequence")
            .isInstanceOf(CharSequence.class);
        Assertions.assertThat(actual.toString())
            .as("string should be equal")
            .isEqualTo(expected.toString());
        break;
      case DATE:
        Assertions.assertThat(expected).as("Should expect a Date").isInstanceOf(LocalDate.class);
        LocalDate date = DateTimeUtil.dateFromDays((int) actual);
        Assertions.assertThat(date).as("date should be equal").isEqualTo(expected);
        break;
      case TIME:
        Assertions.assertThat(expected)
            .as("Should expect a LocalTime")
            .isInstanceOf(LocalTime.class);
        int milliseconds = (int) (((LocalTime) expected).toNanoOfDay() / 1000_000);
        Assertions.assertThat(actual).as("time millis should be equal").isEqualTo(milliseconds);
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          Assertions.assertThat(expected)
              .as("Should expect a OffsetDataTime")
              .isInstanceOf(OffsetDateTime.class);
          OffsetDateTime ts = (OffsetDateTime) expected;
          Assertions.assertThat(((TimestampData) actual).toLocalDateTime())
              .as("OffsetDataTime should be equal")
              .isEqualTo(ts.toLocalDateTime());
        } else {
          Assertions.assertThat(expected)
              .as("Should expect a LocalDataTime")
              .isInstanceOf(LocalDateTime.class);
          LocalDateTime ts = (LocalDateTime) expected;
          Assertions.assertThat(((TimestampData) actual).toLocalDateTime())
              .as("LocalDataTime should be equal")
              .isEqualTo(ts);
        }
        break;
      case BINARY:
        Assertions.assertThat(expected)
            .as("Should expect a ByteBuffer")
            .isInstanceOf(ByteBuffer.class);
        Assertions.assertThat(ByteBuffer.wrap((byte[]) actual))
            .as("binary should be equal")
            .isEqualTo(expected);
        break;
      case DECIMAL:
        Assertions.assertThat(expected)
            .as("Should expect a BigDecimal")
            .isInstanceOf(BigDecimal.class);
        BigDecimal bd = (BigDecimal) expected;
        Assertions.assertThat(((DecimalData) actual).toBigDecimal())
            .as("decimal value should be equal")
            .isEqualTo(bd);
        break;
      case LIST:
        Assertions.assertThat(expected)
            .as("Should expect a Collection")
            .isInstanceOf(Collection.class);
        Collection<?> expectedArrayData = (Collection<?>) expected;
        ArrayData actualArrayData;
        try {
          actualArrayData = (ArrayData) actual;
        } catch (ClassCastException e) {
          actualArrayData = new GenericArrayData((Object[]) actual);
        }
        LogicalType elementType = ((ArrayType) logicalType).getElementType();
        Assertions.assertThat(actualArrayData.size())
            .as("array length should be equal")
            .isEqualTo(expectedArrayData.size());
        assertArrayValues(
            type.asListType().elementType(), elementType, expectedArrayData, actualArrayData);
        break;
      case MAP:
        Assertions.assertThat(expected).as("Should expect a Map").isInstanceOf(Map.class);
        MapData actualMap;
        try {
          actualMap = (MapData) actual;
        } catch (ClassCastException e) {
          actualMap = new GenericMapData((Map<?, ?>) actual);
        }
        assertMapValues(type.asMapType(), logicalType, (Map<?, ?>) expected, actualMap);
        break;
      case STRUCT:
        Assertions.assertThat(expected)
            .as("Should expect a Record")
            .isInstanceOf(GenericData.Record.class);
        assertEquals(
            type.asNestedType().asStructType(), (GenericData.Record) expected, (Row) actual);
        break;
      case UUID:
        Assertions.assertThat(expected).as("Should expect a UUID").isInstanceOf(UUID.class);
        ByteBuffer bb = ByteBuffer.wrap((byte[]) actual);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        Assertions.assertThat(new UUID(firstLong, secondLong).toString())
            .as("UUID should be equal")
            .isEqualTo(expected.toString());
        break;
      case FIXED:
        Assertions.assertThat(expected).as("Should expect byte[]").isInstanceOf(byte[].class);
        Assertions.assertThat((byte[]) actual).as("binary should be equal").isEqualTo(expected);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }

  private static void assertArrayValues(
      Type type, LogicalType logicalType, Collection<?> expectedArray, ArrayData actualArray) {
    List<?> expectedElements = Lists.newArrayList(expectedArray);
    for (int i = 0; i < expectedArray.size(); i += 1) {
      if (expectedElements.get(i) == null) {
        Assertions.assertThat(actualArray.isNullAt(i)).isTrue();
        continue;
      }

      Object expected = expectedElements.get(i);

      assertEquals(
          type,
          logicalType,
          expected,
          ArrayData.createElementGetter(logicalType).getElementOrNull(actualArray, i));
    }
  }

  private static void assertMapValues(
      Types.MapType mapType, LogicalType type, Map<?, ?> expected, MapData actual) {
    Assertions.assertThat(actual.size()).as("map size should be equal").isEqualTo(expected.size());

    ArrayData actualKeyArrayData = actual.keyArray();
    ArrayData actualValueArrayData = actual.valueArray();
    LogicalType actualKeyType = ((MapType) type).getKeyType();
    LogicalType actualValueType = ((MapType) type).getValueType();
    Type keyType = mapType.keyType();
    Type valueType = mapType.valueType();

    ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(actualKeyType);
    ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(actualValueType);

    for (Map.Entry<?, ?> entry : expected.entrySet()) {
      Object matchedActualKey = null;
      int matchedKeyIndex = 0;
      for (int i = 0; i < actual.size(); i += 1) {
        try {
          Object key = keyGetter.getElementOrNull(actualKeyArrayData, i);
          assertEquals(keyType, actualKeyType, entry.getKey(), key);
          matchedActualKey = key;
          matchedKeyIndex = i;
          break;
        } catch (AssertionError e) {
          // not found
        }
      }
      Assertions.assertThat(matchedActualKey).as("Should have a matching key").isNotNull();
      final int valueIndex = matchedKeyIndex;
      assertEquals(
          valueType,
          actualValueType,
          entry.getValue(),
          valueGetter.getElementOrNull(actualValueArrayData, valueIndex));
    }
  }

  public static void assertEquals(ManifestFile expected, ManifestFile actual) {
    if (expected == actual) {
      return;
    }
    Assertions.assertThat(expected != null && actual != null).as("Should not be null.").isTrue();
    Assertions.assertThat(actual.path()).as("Path must match").isEqualTo(expected.path());
    Assertions.assertThat(actual.length()).as("Length must match").isEqualTo(expected.length());
    Assertions.assertThat(actual.partitionSpecId())
        .as("Spec id must match")
        .isEqualTo(expected.partitionSpecId());
    Assertions.assertThat(actual.content())
        .as("ManifestContent must match")
        .isEqualTo(expected.content());
    Assertions.assertThat(actual.sequenceNumber())
        .as("SequenceNumber must match")
        .isEqualTo(expected.sequenceNumber());
    Assertions.assertThat(actual.minSequenceNumber())
        .as("MinSequenceNumber must match")
        .isEqualTo(expected.minSequenceNumber());
    Assertions.assertThat(actual.snapshotId())
        .as("Snapshot id must match")
        .isEqualTo(expected.snapshotId());
    Assertions.assertThat(actual.hasAddedFiles())
        .as("Added files flag must match")
        .isEqualTo(expected.hasAddedFiles());
    Assertions.assertThat(actual.addedFilesCount())
        .as("Added files count must match")
        .isEqualTo(expected.addedFilesCount());
    Assertions.assertThat(actual.addedRowsCount())
        .as("Added rows count must match")
        .isEqualTo(expected.addedRowsCount());
    Assertions.assertThat(actual.hasExistingFiles())
        .as("Existing files flag must match")
        .isEqualTo(expected.hasExistingFiles());
    Assertions.assertThat(actual.existingFilesCount())
        .as("Existing files count must match")
        .isEqualTo(expected.existingFilesCount());
    Assertions.assertThat(actual.existingRowsCount())
        .as("Existing rows count must match")
        .isEqualTo(expected.existingRowsCount());
    Assertions.assertThat(actual.hasDeletedFiles())
        .as("Deleted files flag must match")
        .isEqualTo(expected.hasDeletedFiles());
    Assertions.assertThat(actual.deletedFilesCount())
        .as("Deleted files count must match")
        .isEqualTo(expected.deletedFilesCount());
    Assertions.assertThat(actual.deletedRowsCount())
        .as("Deleted rows count must match")
        .isEqualTo(expected.deletedRowsCount());

    List<ManifestFile.PartitionFieldSummary> expectedSummaries = expected.partitions();
    List<ManifestFile.PartitionFieldSummary> actualSummaries = actual.partitions();
    Assertions.assertThat(actualSummaries.size())
        .as("PartitionFieldSummary size does not match")
        .isEqualTo(expectedSummaries.size());
    for (int i = 0; i < expectedSummaries.size(); i++) {
      Assertions.assertThat(actualSummaries.get(i).containsNull())
          .as("Null flag in partition must match")
          .isEqualTo(expectedSummaries.get(i).containsNull());
      Assertions.assertThat(actualSummaries.get(i).containsNaN())
          .as("NaN flag in partition must match")
          .isEqualTo(expectedSummaries.get(i).containsNaN());
      Assertions.assertThat(actualSummaries.get(i).lowerBound())
          .as("Lower bounds in partition must match")
          .isEqualTo(expectedSummaries.get(i).lowerBound());
      Assertions.assertThat(actualSummaries.get(i).upperBound())
          .as("Upper bounds in partition must match")
          .isEqualTo(expectedSummaries.get(i).upperBound());
    }
  }

  public static void assertEquals(ContentFile<?> expected, ContentFile<?> actual) {
    if (expected == actual) {
      return;
    }
    Assertions.assertThat(expected != null && actual != null).as("Shouldn't be null.").isTrue();
    Assertions.assertThat(actual.specId()).as("SpecId").isEqualTo(expected.specId());
    Assertions.assertThat(actual.content()).as("Content").isEqualTo(expected.content());
    Assertions.assertThat(actual.path()).as("Path").isEqualTo(expected.path());
    Assertions.assertThat(actual.format()).as("Format").isEqualTo(expected.format());
    Assertions.assertThat(actual.partition().size())
        .as("Partition size")
        .isEqualTo(expected.partition().size());

    for (int i = 0; i < expected.partition().size(); i++) {
      Assertions.assertThat(actual.partition().get(i, Object.class))
          .as("Partition data at index " + i)
          .isEqualTo(expected.partition().get(i, Object.class));
    }
    Assertions.assertThat(actual.recordCount())
        .as("Record count")
        .isEqualTo(expected.recordCount());
    Assertions.assertThat(actual.fileSizeInBytes())
        .as("File size in bytes")
        .isEqualTo(expected.fileSizeInBytes());
    Assertions.assertThat(actual.columnSizes())
        .as("Column sizes")
        .isEqualTo(expected.columnSizes());
    Assertions.assertThat(actual.valueCounts())
        .as("Value counts")
        .isEqualTo(expected.valueCounts());
    Assertions.assertThat(actual.nullValueCounts())
        .as("Null value counts")
        .isEqualTo(expected.nullValueCounts());
    Assertions.assertThat(actual.lowerBounds())
        .as("Lower bounds")
        .isEqualTo(expected.lowerBounds());
    Assertions.assertThat(actual.upperBounds())
        .as("Upper bounds")
        .isEqualTo(expected.upperBounds());
    Assertions.assertThat(actual.keyMetadata())
        .as("Key metadata")
        .isEqualTo(expected.keyMetadata());
    Assertions.assertThat(actual.splitOffsets())
        .as("Split offsets")
        .isEqualTo(expected.splitOffsets());
    Assertions.assertThat(actual.equalityFieldIds())
        .as("Equality field id list")
        .isEqualTo(expected.equalityFieldIds());
  }
}
