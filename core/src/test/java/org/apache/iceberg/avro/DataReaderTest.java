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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
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
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DataReaderTest {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "timestamp", Types.TimestampType.withoutZone()));

  private List<Record> records;
  private LocalDateTime baseTime;

  @TempDir Path temp;

  @BeforeEach
  public void createRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA);
    baseTime = LocalDateTime.of(2023, 6, 1, 0, 0); // Use a fixed base time for consistent testing

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    for (int i = 0; i < 5; i++) {
      builder.add(
          record.copy(
              ImmutableMap.of(
                  "id", (long) (i + 1),
                  "data", String.valueOf((char) ('a' + i)),
                  "timestamp", baseTime.plusMinutes(i))));
    }

    this.records = builder.build();
  }

  // START GENAI
  @Test
  public void testDataWriter() throws IOException {
    OutputFile file = Files.localOutput(temp.toFile());

    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).withOrderId(10).asc("id").build();

    DataWriter<Record> dataWriter =
        Avro.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
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

    assertThat(dataFile.format()).as("Format should be Avro").isEqualTo(FileFormat.AVRO);
    assertThat(dataFile.content()).as("Should be data file").isEqualTo(FileContent.DATA);
    assertThat(dataFile.recordCount()).as("Record count should match").isEqualTo(records.size());
    assertThat(dataFile.partition().size()).as("Partition should be empty").isEqualTo(0);
    assertThat(dataFile.sortOrderId()).as("Sort order should match").isEqualTo(sortOrder.orderId());
    assertThat(dataFile.keyMetadata()).as("Key metadata should be null").isNull();

    List<Record> writtenRecords;
    try (AvroIterable<Record> reader =
        Avro.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(DataReader::create)
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    assertThat(writtenRecords).as("Written records should match").hasSameSizeAs(records);

    for (int i = 0; i < records.size(); i++) {
      Record originalRecord = records.get(i);
      Record writtenRecord = writtenRecords.get(i);

      assertThat(writtenRecord.get(0)).as("ID should match").isEqualTo(originalRecord.get(0));
      assertThat(writtenRecord.get(1)).as("Data should match").isEqualTo(originalRecord.get(1));

      // Check if the timestamp is correctly read back
      LocalDateTime originalTimestamp = (LocalDateTime) originalRecord.get(2);
      LocalDateTime writtenTimestamp = (LocalDateTime) writtenRecord.get(2);

      assertThat(writtenTimestamp).as("Timestamp should match").isEqualTo(originalTimestamp);

      // Verify that the timestamp is correct
      LocalDateTime expectedDateTime = baseTime.plusMinutes(i);
      assertThat(writtenTimestamp)
          .as("Timestamp should match expected value")
          .isEqualTo(expectedDateTime);

      // Verify that the timestamp is stored with microsecond precision
      assertThat(writtenTimestamp.getNano() % 1000)
          .as("Timestamp should have microsecond precision")
          .isEqualTo(0);
    }
  }
}
