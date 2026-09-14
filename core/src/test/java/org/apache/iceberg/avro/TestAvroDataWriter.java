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
import java.nio.ByteBuffer;
import java.nio.file.Path;
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
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestAvroDataWriter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private List<Record> records;

  @TempDir Path temp;

  @BeforeEach
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
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    assertThat(writtenRecords).as("Written records should match").isEqualTo(records);
  }

  @Test
  public void testGeospatialWkbRoundTrip() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "geom", Types.GeometryType.crs84()),
            Types.NestedField.optional(3, "geog", Types.GeographyType.crs84()));

    GenericRecord record = GenericRecord.create(schema);
    List<Record> geoRecords =
        ImmutableList.of(
            record.copy(
                ImmutableMap.of("id", 1L, "geom", wkbPoint(30, 10), "geog", wkbPoint(-5, 40))),
            // geog is left null
            record.copy(ImmutableMap.of("id", 2L, "geom", wkbPoint(0, 0))),
            // both geo columns are left null
            record.copy(ImmutableMap.of("id", 3L)));

    OutputFile file = Files.localOutput(temp.toFile());
    DataWriter<Record> dataWriter =
        Avro.writeData(file)
            .schema(schema)
            .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();
    try (dataWriter) {
      for (Record geoRecord : geoRecords) {
        dataWriter.write(geoRecord);
      }
    }

    assertThat(dataWriter.toDataFile().recordCount()).isEqualTo(geoRecords.size());

    List<Record> writtenRecords;
    try (AvroIterable<Record> reader =
        Avro.read(file.toInputFile())
            .project(schema)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }

    assertThat(writtenRecords)
        .as("Geometry and geography WKB should round-trip through Avro")
        .isEqualTo(geoRecords);
  }

  private static ByteBuffer wkbPoint(double xCoord, double yCoord) {
    return ByteBuffer.wrap(RandomUtil.wkbPoint(xCoord, yCoord));
  }
}
