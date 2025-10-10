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
package org.apache.iceberg.data.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

public class TestGeographyReadersAndWriters {
  private final Schema schema;
  private static final Configuration CONF = new Configuration();

  @TempDir Path tempDir;

  private final List<Record> testData;

  public TestGeographyReadersAndWriters() {
    this.schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "geog", Types.GeographyType.crs84()));
    testData = prepareTestData();
  }

  private List<Record> prepareTestData() {
    List<Record> recordList = Lists.newArrayList();
    GeometryFactory factory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();
    for (long id = 0; id < 1000; id++) {
      // lng: -100 to 100, lat: -50 to 50
      double lng = id * 0.2 - 100;
      double lat = id * 0.1 - 50;
      Coordinate center = new Coordinate(lng, lat);
      byte[] wkb = wkbWriter.write(factory.createPoint(center));
      ByteBuffer geog = ByteBuffer.wrap(wkb);
      Record record = GenericRecord.create(schema);
      record.setField("id", id);
      record.setField("geog", geog);
      recordList.add(record);
    }
    return recordList;
  }

  @Test
  public void testWriteAndReadGeometryValues() throws IOException, ParseException {
    // Create a table
    File location = tempDir.toFile();
    Table table =
        TestTables.create(
            location,
            "geog_table",
            schema,
            PartitionSpec.unpartitioned(),
            3,
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

    // Write some data
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
    Path path = tempDir.resolve("data.parquet");
    OutputFile outputFile =
        HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(path.toString()), CONF);
    try (var fileAppender = appenderFactory.newAppender(outputFile, FileFormat.PARQUET)) {
      fileAppender.addAll(testData);
      fileAppender.close();
      Metrics metrics = fileAppender.metrics();

      // Commit the data file to the table
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withInputFile(
                  HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path.toString()), CONF))
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics)
              .build();
      table.newAppend().appendFile(dataFile).commit();

      // Read the data
      WKBReader wkbReader = new WKBReader();
      try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
        int numRecords = 0;
        for (Record record : reader) {
          ByteBuffer geogWkb = (ByteBuffer) record.getField("geog");
          Geometry geometry = wkbReader.read(ByteBuffers.toByteArray(geogWkb));
          assertThat(geometry).isInstanceOf(Point.class);
          numRecords++;
        }
        assertThat(numRecords).as("Record count must match").isEqualTo(testData.size());
      }
    }
  }
}
