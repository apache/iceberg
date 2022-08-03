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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetDataWriter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()));

  private List<Record> records;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Before
  public void createRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(record.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(record.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(record.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(record.copy(ImmutableMap.of("id", 5L, "data", "e")));

    this.records = builder.build();
  }

  @Test
  public void testDataWriter() throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());

    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).withOrderId(10).asc("id").build();

    DataWriter<Record> dataWriter =
        Parquet.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .withSortOrder(sortOrder)
            .build();

    try {
      for (Record record : records) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();

    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, dataFile.format());
    Assert.assertEquals("Should be data file", FileContent.DATA, dataFile.content());
    Assert.assertEquals("Record count should match", records.size(), dataFile.recordCount());
    Assert.assertEquals("Partition should be empty", 0, dataFile.partition().size());
    Assert.assertEquals(
        "Sort order should match", sortOrder.orderId(), (int) dataFile.sortOrderId());
    Assert.assertNull("Key metadata should be null", dataFile.keyMetadata());

    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Written records should match", records, writtenRecords);
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @Test
  public void testInvalidUpperBoundString() throws Exception {
    OutputFile file = Files.localOutput(temp.newFile());

    Table testTable =
        TestTables.create(
            temp.newFile(),
            "test_invalid_string_bound",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            2);
    testTable
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)")
        .commit();

    DataWriter<Record> dataWriter =
        Parquet.writeData(file)
            .metricsConfig(MetricsConfig.forTable(testTable))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    // These high code points cause an overflow
    GenericRecord genericRecord = GenericRecord.create(SCHEMA);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    char[] charArray = new char[61];
    for (int i = 0; i < 60; i = i + 2) {
      charArray[i] = '\uDBFF';
      charArray[i + 1] = '\uDFFF';
    }
    builder.add(genericRecord.copy(ImmutableMap.of("id", 1L, "data", String.valueOf(charArray))));
    List<Record> overflowRecords = builder.build();

    try {
      for (Record record : overflowRecords) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();

    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, dataFile.format());
    Assert.assertEquals("Should be data file", FileContent.DATA, dataFile.content());
    Assert.assertEquals(
        "Record count should match", overflowRecords.size(), dataFile.recordCount());
    Assert.assertEquals("Partition should be empty", 0, dataFile.partition().size());
    Assert.assertNull("Key metadata should be null", dataFile.keyMetadata());

    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Written records should match", overflowRecords, writtenRecords);

    Assert.assertTrue("Should have a valid lower bound", dataFile.lowerBounds().containsKey(1));
    Assert.assertTrue("Should have a valid upper bound", dataFile.upperBounds().containsKey(1));
    Assert.assertTrue("Should have a valid lower bound", dataFile.lowerBounds().containsKey(2));
    Assert.assertFalse("Should have a null upper bound", dataFile.upperBounds().containsKey(2));
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @Test
  public void testInvalidUpperBoundBinary() throws Exception {
    OutputFile file = Files.localOutput(temp.newFile());

    Table testTable =
        TestTables.create(
            temp.newFile(),
            "test_invalid_binary_bound",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            2);
    testTable
        .updateProperties()
        .set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "truncate(16)")
        .commit();

    DataWriter<Record> dataWriter =
        Parquet.writeData(file)
            .metricsConfig(MetricsConfig.forTable(testTable))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    // This max binary value causes an overflow
    GenericRecord genericRecord = GenericRecord.create(SCHEMA);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    ByteBuffer bytes = ByteBuffer.allocate(17);
    for (int i = 0; i < 17; i++) {
      bytes.put(i, (byte) 0xff);
    }
    builder.add(genericRecord.copy(ImmutableMap.of("id", 1L, "binary", bytes)));
    List<Record> overflowRecords = builder.build();

    try {
      for (Record record : overflowRecords) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();

    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, dataFile.format());
    Assert.assertEquals("Should be data file", FileContent.DATA, dataFile.content());
    Assert.assertEquals(
        "Record count should match", overflowRecords.size(), dataFile.recordCount());
    Assert.assertEquals("Partition should be empty", 0, dataFile.partition().size());
    Assert.assertNull("Key metadata should be null", dataFile.keyMetadata());

    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Written records should match", overflowRecords, writtenRecords);

    Assert.assertTrue("Should have a valid lower bound", dataFile.lowerBounds().containsKey(1));
    Assert.assertTrue("Should have a valid upper bound", dataFile.upperBounds().containsKey(1));
    Assert.assertTrue("Should have a valid lower bound", dataFile.lowerBounds().containsKey(3));
    Assert.assertFalse("Should have a null upper bound", dataFile.upperBounds().containsKey(3));
  }
}
