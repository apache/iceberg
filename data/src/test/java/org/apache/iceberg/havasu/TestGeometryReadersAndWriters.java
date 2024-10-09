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
package org.apache.iceberg.havasu;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Tables;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
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
import org.apache.iceberg.types.havasu.GeometryEncoding;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

@RunWith(Parameterized.class)
public class TestGeometryReadersAndWriters {

  @Parameterized.Parameters(name = "encoding = {0}")
  public static Object[] parameters() {
    return new Object[] {"ewkb", "wkb", "wkt", "geojson"};
  }

  private final Schema schema;
  private static final Configuration CONF = new Configuration();
  private static final Tables TABLES = new HadoopTables(CONF);

  @Rule public final TemporaryFolder temp = new TemporaryFolder();

  private static final GeometryFactory factory = new GeometryFactory();

  private final List<List<Record>> testData;

  public TestGeometryReadersAndWriters(String encodingName) {
    GeometryEncoding encoding = GeometryEncoding.fromName(encodingName);
    this.schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "part", Types.IntegerType.get()),
            optional(3, "geom", Types.GeometryType.get(encoding)));
    testData = prepareTestData();
  }

  private List<List<Record>> prepareTestData() {
    // Test data contains records for four data files, where each data file contain a set of
    // geometries
    // in a different quadrant of the cartesian plane.
    List<List<Record>> recordsInDataFiles = Lists.newArrayList();
    long id = 0;
    for (int i = 0; i < 4; i++) {
      List<Record> recordList = Lists.newArrayList();
      for (int k = 1; k <= 10; k++) {
        Coordinate center = null;
        switch (i) {
          case 0:
            center = new Coordinate(k, k);
            break;
          case 1:
            center = new Coordinate(-k, k);
            break;
          case 2:
            center = new Coordinate(-k, -k);
            break;
          case 3:
            center = new Coordinate(k, -k);
            break;
        }
        Point point = factory.createPoint(center);
        Geometry polygon = point.buffer(0.5);

        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("part", i);
        record.setField("geom", point);
        recordList.add(record);

        record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("part", i);
        record.setField("geom", polygon);
        recordList.add(record);

        id += 1;
      }
      recordsInDataFiles.add(recordList);
    }
    return recordsInDataFiles;
  }

  @Test
  public void testWriteAndReadGeometryValues() throws IOException {
    // Create a table
    File location = temp.newFolder("geos-table-1");
    Assert.assertTrue(location.delete());
    Table table =
        TABLES.create(
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            location.toString());

    // Write some data
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
    Path path = new Path(location.toString(), "data.parquet");
    OutputFile outputFile = HadoopOutputFile.fromPath(path, CONF);
    FileAppender<Record> fileAppender = appenderFactory.newAppender(outputFile, FileFormat.PARQUET);
    fileAppender.addAll(testData.get(0));
    fileAppender.close();

    // Validate that the metrics were populated correctly
    Metrics metrics = fileAppender.metrics();
    Assert.assertEquals(3, metrics.lowerBounds().size());
    Assert.assertEquals(3, metrics.upperBounds().size());

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
        Assert.assertTrue(record.getField("geom") instanceof Geometry);
        numRecords += 1;
      }
    }
    Assert.assertEquals(20, numRecords);
  }

  @Test
  public void testFilterTableWithSpatialPredicates() throws IOException {
    // Create a table
    File location = temp.newFolder("geos-table-2");
    Assert.assertTrue(location.delete());
    Table table =
        TABLES.create(
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            location.toString());

    // Write records into 4 data files
    AppendFiles append = table.newAppend();
    for (int i = 0; i < testData.size(); i++) {
      List<Record> fileContent = testData.get(i);
      GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
      Path path = new Path(location.toString(), "data-" + i + ".parquet");
      OutputFile outputFile = HadoopOutputFile.fromPath(path, CONF);
      FileAppender<Record> fileAppender =
          appenderFactory.newAppender(outputFile, FileFormat.PARQUET);
      fileAppender.addAll(fileContent);
      fileAppender.close();
      Metrics metrics = fileAppender.metrics();
      DataFile dataFile =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withInputFile(HadoopInputFile.fromPath(path, CONF))
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics)
              .build();
      append.appendFile(dataFile);
    }
    append.commit();

    // Read the table with spatial predicates
    // test the intersects predicate
    Expression expr = Expressions.stIntersects("geom", factory.createPoint(new Coordinate(1, 1)));
    validateScan(table, expr, 1, 2);
    expr = Expressions.stIntersects("geom", factory.createPoint(new Coordinate(0, 0)));
    validateScan(table, expr, 0, 0);
    expr = Expressions.stIntersects("geom", factory.createPoint(new Coordinate(1.5, 1.5)));
    validateScan(table, expr, 1, 0);
    expr = Expressions.stIntersects("geom", factory.toGeometry(new Envelope(0.5, 1.1, -1.1, 1.1)));
    validateScan(table, expr, 2, 4);
    expr = Expressions.stIntersects("geom", factory.toGeometry(new Envelope(0, 0.75, 0, 0.75)));
    validateScan(table, expr, 1, 1);
    expr =
        Expressions.stIntersects("geom", factory.toGeometry(new Envelope(0.75, 1.25, 0.75, 1.25)));
    validateScan(table, expr, 1, 2);

    // test the covers predicate
    expr = Expressions.stCovers("geom", factory.createPoint(new Coordinate(1, 1)));
    validateScan(table, expr, 1, 2);
    expr = Expressions.stCovers("geom", factory.createPoint(new Coordinate(0, 0)));
    validateScan(table, expr, 0, 0);
    expr = Expressions.stCovers("geom", factory.createPoint(new Coordinate(1.5, 1.5)));
    validateScan(table, expr, 1, 0);
    expr = Expressions.stCovers("geom", factory.toGeometry(new Envelope(0, 0.75, 0, 0.75)));
    validateScan(table, expr, 0, 0);
    expr = Expressions.stCovers("geom", factory.toGeometry(new Envelope(0.75, 1.25, 0.75, 1.25)));
    validateScan(table, expr, 1, 1);

    // test spatial predicate and other predicate mixed together
    expr =
        Expressions.and(
            Expressions.lessThanOrEqual("id", 10L),
            Expressions.stIntersects(
                "geom", factory.toGeometry(new Envelope(0.5, 1.1, -1.1, 1.1))));
    validateScan(table, expr, 1, 2);
  }

  @Test
  public void testPartitionedGeometryTable() throws IOException {
    // Create a table
    File location = temp.newFolder("geos-table-2-partitioned");
    Assert.assertTrue(location.delete());
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("part").build();
    Table table =
        TABLES.create(
            schema,
            spec,
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"),
            location.toString());

    // Write records into 4 data files, with 4 partitions
    AppendFiles append = table.newAppend();
    for (int i = 0; i < testData.size(); i++) {
      List<Record> fileContent = testData.get(i);
      GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema());
      Path path = new Path(location.toString(), "data-" + i + ".parquet");
      OutputFile outputFile = HadoopOutputFile.fromPath(path, CONF);
      FileAppender<Record> fileAppender =
          appenderFactory.newAppender(outputFile, FileFormat.PARQUET);
      fileAppender.addAll(fileContent);
      fileAppender.close();
      Metrics metrics = fileAppender.metrics();
      PartitionData partition = new PartitionData(spec.partitionType());
      partition.set(0, i);
      DataFile dataFile =
          DataFiles.builder(spec)
              .withInputFile(HadoopInputFile.fromPath(path, CONF))
              .withPartition(partition)
              .withFormat(FileFormat.PARQUET)
              .withMetrics(metrics)
              .build();
      append.appendFile(dataFile);
    }
    append.commit();

    // Read the table with spatial predicates
    // test the intersects predicate
    Expression expr = Expressions.stIntersects("geom", factory.createPoint(new Coordinate(1, 1)));
    validateScan(table, expr, 1, 2);

    // test spatial predicate and partition condition mixed together
    expr =
        Expressions.and(
            Expressions.equal("part", 3),
            Expressions.stIntersects(
                "geom", factory.toGeometry(new Envelope(0.5, 1.1, -1.1, 1.1))));
    validateScan(table, expr, 1, 2);
  }

  @SuppressWarnings("unchecked")
  private List<Record> validateScan(
      Table table, Expression expr, int expectedScannedFileNum, int expectedResultNum)
      throws IOException {
    List<Record> results;
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).where(expr).build()) {
      try (CloseableIterator<Record> iter = reader.iterator()) {
        results = (List<Record>) IteratorUtils.toList(iter);
        Assert.assertEquals(expectedResultNum, results.size());
      }
    }
    TableScan filteredScan = table.newScan().filter(expr);
    try (CloseableIterable<FileScanTask> planFiles = filteredScan.planFiles()) {
      try (CloseableIterator<FileScanTask> fileScanTasks = planFiles.iterator()) {
        int numScannedFiles = IteratorUtils.toList(fileScanTasks).size();
        Assert.assertEquals(expectedScannedFileNum, numScannedFiles);
      }
    }
    return results;
  }
}
