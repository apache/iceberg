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
package org.apache.iceberg.orc;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcConf;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestOrcDataReader {
  @ClassRule public static TemporaryFolder temp = new TemporaryFolder();

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()));
  private static DataFile dataFile;
  private static OutputFile outputFile;

  @BeforeClass
  public static void createDataFile() throws IOException {
    GenericRecord bufferRecord = GenericRecord.create(SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(bufferRecord.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(bufferRecord.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(bufferRecord.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(bufferRecord.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(bufferRecord.copy(ImmutableMap.of("id", 5L, "data", "e")));

    outputFile = Files.localOutput(File.createTempFile("test", ".orc", temp.getRoot()));

    DataWriter<Record> dataWriter =
        ORC.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    try {
      for (Record record : builder.build()) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    dataFile = dataWriter.toDataFile();
  }

  @Test
  public void testWrite() {
    Assert.assertEquals("Format should be ORC", FileFormat.ORC, dataFile.format());
    Assert.assertEquals("Should be data file", FileContent.DATA, dataFile.content());
    Assert.assertEquals("Record count should match", 5, dataFile.recordCount());
    Assert.assertEquals("Partition should be empty", 0, dataFile.partition().size());
    Assert.assertNull("Key metadata should be null", dataFile.keyMetadata());
  }

  private void validateAllRecords(List<Record> records) {
    Assert.assertEquals(5, records.size());
    long id = 1;
    char data = 'a';
    for (Record record : records) {
      Assert.assertEquals(id, record.getField("id"));
      id++;
      Assert.assertEquals(data, ((String) record.getField("data")).charAt(0));
      data++;
    }
  }

  @Test
  public void testRowReader() throws IOException {
    try (CloseableIterable<Record> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.and(Expressions.notNull("data"), Expressions.equal("id", 3L)))
            .build()) {
      validateAllRecords(Lists.newArrayList(reader));
    }
  }

  @Test
  public void testRowReaderWithFilter() throws IOException {
    try (CloseableIterable<Record> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.and(Expressions.notNull("data"), Expressions.equal("id", 3L)))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.name(), String.valueOf(true))
            .build()) {
      validateAllRecords(Lists.newArrayList(reader));
    }
  }

  @Test
  public void testRowReaderWithFilterWithSelected() throws IOException {
    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        ORC.read(outputFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .filter(Expressions.and(Expressions.notNull("data"), Expressions.equal("id", 3L)))
            .config(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), String.valueOf(true))
            .config(OrcConf.READER_USE_SELECTED.getAttribute(), String.valueOf(true))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    Assert.assertEquals(1, writtenRecords.size());
    Assert.assertEquals(3L, writtenRecords.get(0).get(0));
    Assert.assertEquals("c", writtenRecords.get(0).get(1));
  }
}
