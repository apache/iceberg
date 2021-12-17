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
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetDataWriter {
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "data", Types.StringType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testDataWriter() throws IOException {
    OutputFile file = Files.localOutput(temp.newFile());

    SortOrder sortOrder = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("id")
        .build();

    DataWriter<Record> dataWriter = Parquet.writeData(file)
        .schema(SCHEMA)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .withSortOrder(sortOrder)
        .build();

    GenericRecord genericRecord = GenericRecord.create(SCHEMA);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(genericRecord.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(genericRecord.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(genericRecord.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(genericRecord.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(genericRecord.copy(ImmutableMap.of("id", 5L, "data", "e")));
    List<Record> records = builder.build();

    try {
      for (Record record : records) {
        dataWriter.add(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();

    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, dataFile.format());
    Assert.assertEquals("Should be data file", FileContent.DATA, dataFile.content());
    Assert.assertEquals("Record count should match", records.size(), dataFile.recordCount());
    Assert.assertEquals("Partition should be empty", 0, dataFile.partition().size());
    Assert.assertEquals("Sort order should match", sortOrder.orderId(), (int) dataFile.sortOrderId());
    Assert.assertNull("Key metadata should be null", dataFile.keyMetadata());

    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader = Parquet.read(file.toInputFile())
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
        .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Written records should match", records, writtenRecords);
  }

  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  @Test
  public void testCorruptString() throws Exception {
    OutputFile file = Files.localOutput(temp.newFile());

    SortOrder sortOrder = SortOrder.builderFor(SCHEMA)
        .withOrderId(10)
        .asc("id")
        .build();

    DataWriter<Record> dataWriter = Parquet.writeData(file)
        .schema(SCHEMA)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .withSortOrder(sortOrder)
        .build();

    GenericRecord genericRecord = GenericRecord.create(SCHEMA);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    char[] charArray = new char[61];
    for (int i = 0; i < 60; i = i + 2) {
      charArray[i] = '\uDBFF';
      charArray[i + 1] = '\uDFFF';
    }
    builder.add(genericRecord.copy(ImmutableMap.of("id", 1L, "data", String.valueOf(charArray))));
    List<Record> records = builder.build();

    try {
      for (Record record : records) {
        dataWriter.add(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();

    Assert.assertEquals("Format should be Parquet", FileFormat.PARQUET, dataFile.format());
    Assert.assertEquals("Should be data file", FileContent.DATA, dataFile.content());
    Assert.assertEquals("Record count should match", records.size(), dataFile.recordCount());
    Assert.assertEquals("Partition should be empty", 0, dataFile.partition().size());
    Assert.assertEquals("Sort order should match", sortOrder.orderId(), (int) dataFile.sortOrderId());
    Assert.assertNull("Key metadata should be null", dataFile.keyMetadata());

    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader = Parquet.read(file.toInputFile())
        .project(SCHEMA)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
        .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals("Written records should match", records, writtenRecords);

    Assert.assertTrue("Should have a valid lower bound", dataFile.lowerBounds().containsKey(1));
    Assert.assertTrue("Should have a valid upper bound", dataFile.upperBounds().containsKey(1));
    Assert.assertTrue("Should have a valid lower bound", dataFile.lowerBounds().containsKey(2));
    Assert.assertFalse("Should not have found a valid upper bound", dataFile.upperBounds().containsKey(2));
  }
}
