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
package org.apache.iceberg.parquet;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public final class Int96TestUtil {
  private static final BigInteger NANOS_PER_DAY = BigInteger.valueOf(86_400_000_000_000L);
  private static final BigInteger EPOCH_JULIAN = BigInteger.valueOf(2_440_588L);
  private static final BigInteger NANOS_PER_MICRO = BigInteger.valueOf(1_000L);
  private static final int ROW_GROUP_SIZE = 16;
  private static final MessageType FILE_SCHEMA =
      MessageTypeParser.parseMessageType(
          "message int96 { required int32 id = 1; optional int96 ts = 2; "
              + "optional group nested = 3 { optional int96 ts = 4; } "
              + "optional group items (LIST) = 5 { repeated group list { "
              + "optional int96 element = 6; } } "
              + "optional group attributes (MAP) = 7 { repeated group key_value { "
              + "required binary key (STRING) = 8; optional int96 value = 9; } } }");

  private Int96TestUtil() {}

  public record Row(
      int id, BigInteger timestampNanos, boolean parentPresent, BigInteger nestedTimestampNanos) {}

  public static File write(Path file, boolean dictionary, List<BigInteger> epochNanos)
      throws IOException {
    List<Row> rows =
        IntStream.range(0, epochNanos.size())
            .mapToObj(index -> new Row(index, epochNanos.get(index), false, null))
            .toList();

    return writeRows(file, dictionary, rows);
  }

  public static File writeRows(Path file, boolean dictionary, List<Row> rows) throws IOException {
    return writeRows(file, dictionary, rows, 1024 * 1024, ROW_GROUP_SIZE);
  }

  public static File writeMixedEncoding(Path file, List<BigInteger> epochNanos) throws IOException {
    List<Row> rows =
        IntStream.range(0, epochNanos.size())
            .mapToObj(index -> new Row(index, epochNanos.get(index), false, null))
            .toList();

    File written = writeRows(file, true, rows, 64, Math.max(ROW_GROUP_SIZE, rows.size()));
    assertEncoding(written, true, true);
    return written;
  }

  private static File writeRows(
      Path file, boolean dictionary, List<Row> rows, int dictionaryPageSize, int rowGroupSize)
      throws IOException {
    SimpleGroupFactory groups = new SimpleGroupFactory(FILE_SCHEMA);
    boolean hasTimestamp = false;
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new LocalOutputFile(file))
            .withType(FILE_SCHEMA)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withDictionaryEncoding(dictionary)
            .withDictionaryPageSize(dictionaryPageSize)
            .withRowGroupRowCountLimit(rowGroupSize)
            .withPageRowCountLimit(12)
            .withMinRowCountForPageSizeCheck(1)
            .withMaxRowCountForPageSizeCheck(4)
            .withWriterVersion(WriterVersion.PARQUET_1_0)
            .build()) {
      for (Row row : rows) {
        Group group = groups.newGroup().append("id", row.id());
        if (row.timestampNanos() != null) {
          group.add("ts", encode(row.timestampNanos()));
          hasTimestamp = true;
        }

        if (row.parentPresent()) {
          Group nested = group.addGroup("nested");
          Group items = group.addGroup("items");
          Group element = items.addGroup("list");
          items.addGroup("list");
          Group attributes = group.addGroup("attributes");
          Group value = attributes.addGroup("key_value").append("key", "value");
          attributes.addGroup("key_value").append("key", "null");
          if (row.nestedTimestampNanos() != null) {
            nested.add("ts", encode(row.nestedTimestampNanos()));
            element.add("element", encode(row.nestedTimestampNanos()));
            value.add("value", encode(row.nestedTimestampNanos()));
            hasTimestamp = true;
          }
        }

        writer.write(group);
      }
    }

    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      int expectedGroups = (rows.size() + rowGroupSize - 1) / rowGroupSize;
      if (reader.getFooter().getBlocks().size() != expectedGroups) {
        throw new AssertionError("Unexpected INT96 row-group count in " + file);
      }
    }

    if (hasTimestamp) {
      assertEncoding(file.toFile(), dictionary);
    }

    return file.toFile();
  }

  public static Binary encode(BigInteger epochNanos) {
    BigInteger[] dayAndNanos = epochNanos.divideAndRemainder(NANOS_PER_DAY);
    if (dayAndNanos[1].signum() < 0) {
      dayAndNanos[0] = dayAndNanos[0].subtract(BigInteger.ONE);
      dayAndNanos[1] = dayAndNanos[1].add(NANOS_PER_DAY);
    }

    ByteBuffer bytes = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    bytes.putLong(dayAndNanos[1].longValueExact());
    bytes.putInt(dayAndNanos[0].add(EPOCH_JULIAN).intValueExact());
    return Binary.fromConstantByteArray(bytes.array());
  }

  public static long expectedTicks(BigInteger epochNanos, boolean nanos) {
    BigInteger[] ticksAndRemainder =
        epochNanos.divideAndRemainder(nanos ? BigInteger.ONE : NANOS_PER_MICRO);
    if (ticksAndRemainder[1].signum() < 0) {
      ticksAndRemainder[0] = ticksAndRemainder[0].subtract(BigInteger.ONE);
    }

    return ticksAndRemainder[0].longValueExact();
  }

  public static void assertEncoding(File file, boolean dictionary) throws IOException {
    assertEncoding(file, dictionary, false);
  }

  private static void assertEncoding(File file, boolean dictionary, boolean mixed)
      throws IOException {
    EnumSet<Encoding> encodings = EnumSet.noneOf(Encoding.class);
    boolean hasMixedColumnChunk = false;
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file.toPath()))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        try (PageReadStore pagesInGroup = rowGroup) {
          for (ColumnDescriptor column : schema.getColumns()) {
            if (column.getPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.INT96) {
              continue;
            }

            EnumSet<Encoding> columnEncodings = encodings(pagesInGroup.getPageReader(column));
            encodings.addAll(columnEncodings);

            hasMixedColumnChunk |=
                column.getPath().length == 1
                    && column.getPath()[0].equals("ts")
                    && columnEncodings.stream().anyMatch(Encoding::usesDictionary)
                    && columnEncodings.contains(Encoding.PLAIN);
          }
        }
      }
    }

    if (dictionary != encodings.stream().anyMatch(Encoding::usesDictionary)) {
      throw new AssertionError("Unexpected INT96 data-page encoding in " + file);
    }

    if (!dictionary && !encodings.contains(Encoding.PLAIN)) {
      throw new AssertionError("Expected PLAIN INT96 data pages in " + file);
    }

    if (mixed && !hasMixedColumnChunk) {
      throw new AssertionError(
          "Expected dictionary and PLAIN INT96 data pages in one column chunk");
    }
  }

  private static EnumSet<Encoding> encodings(PageReader pages) {
    EnumSet<Encoding> encodings = EnumSet.noneOf(Encoding.class);
    DataPage page;
    while ((page = pages.readPage()) != null) {
      encodings.add(
          page instanceof DataPageV1
              ? ((DataPageV1) page).getValueEncoding()
              : ((DataPageV2) page).getDataEncoding());
    }

    return encodings;
  }
}
