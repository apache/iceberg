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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Geography;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class TestGeographyReadersAndWriters {
  private final Schema schema;
  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);

  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  private static final GeometryFactory FACTORY = new GeometryFactory();

  private final List<Record> testData;

  public TestGeographyReadersAndWriters() {
    this.schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "geog", Types.GeographyType.get()));
    testData = prepareTestData();
  }

  private List<Record> prepareTestData() {
    // Test data contains records for four data files, where each data file contain a set of
    // geometries in a different quadrant of the cartesian plane.
    List<List<Record>> recordsInDataFiles = Lists.newArrayList();
    List<Record> recordList = Lists.newArrayList();
    for (long id = 0; id < 1000; id++) {
      // lng: -100 to 100, lat: -50 to 50
      double lng = id * 0.2 - 100;
      double lat = id * 0.1 - 50;
      Coordinate center = new Coordinate(lng, lat);

      Geography geog = new Geography(FACTORY.createPoint(center));
      Record record = GenericRecord.create(schema);
      record.setField("id", id);
      record.setField("geog", geog);
      recordList.add(record);
    }
    return recordList;
  }

  @Test
  public void testWriteAndReadGeometryValues() throws IOException {
    // Create a table
    File location = temp.newFolder("geog-table-1");
    Assert.assertTrue(location.delete());
    Table table =
        TABLES.create(
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                "3",
                TableProperties.DEFAULT_FILE_FORMAT,
                "parquet"),
            location.toString());

    // Write some data
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
    Path path = new Path(location.toString(), "data.parquet");
    OutputFile outputFile = HadoopOutputFile.fromPath(path, CONF);
    FileAppender<Record> fileAppender = appenderFactory.newAppender(outputFile, FileFormat.PARQUET);
    fileAppender.addAll(testData);
    fileAppender.close();

    Metrics metrics = fileAppender.metrics();

    // Commit the data file to the table
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(HadoopInputFile.fromPath(path, CONF))
            .withFormat(FileFormat.PARQUET)
            .withMetrics(metrics)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    // Read the data
    CloseableIterable<Record> reader = IcebergGenerics.read(table).build();
    int numRecords = 0;
    try (CloseableIterator<Record> iter = reader.iterator()) {
      while (iter.hasNext()) {
        Record record = iter.next();
        Assert.assertTrue(record.getField("geog") instanceof Geography);
        numRecords += 1;
      }
    }
    Assert.assertEquals(testData.size(), numRecords);
  }
}
