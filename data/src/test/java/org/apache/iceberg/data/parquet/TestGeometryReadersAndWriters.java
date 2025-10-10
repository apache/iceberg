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
import org.apache.iceberg.TestTables;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.geospatial.BoundingBox;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

public class TestGeometryReadersAndWriters {

  private final Schema schema;
  private static final Configuration CONF = new Configuration();
  private final List<List<Record>> testData;

  @TempDir java.nio.file.Path tempDir;

  public TestGeometryReadersAndWriters() {
    this.schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "part", Types.IntegerType.get()),
            Types.NestedField.optional(3, "geom", Types.GeometryType.crs84()));
    testData = prepareTestData();
  }

  private List<List<Record>> prepareTestData() {
    List<List<Record>> recordsInDataFiles = Lists.newArrayList();
    GeometryFactory factory = new GeometryFactory();
    WKBWriter wkbWriter = new WKBWriter();
    long id = 0;
    for (int i = 0; i < 4; i++) {
      List<Record> recordList = Lists.newArrayList();
      for (int k = 1; k <= 10; k++) {
        Coordinate center;
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
          default:
            center = new Coordinate(k, -k);
            break;
        }
        byte[] pointWkb = wkbWriter.write(factory.createPoint(center));
        ByteBuffer pointBuffer = ByteBuffer.wrap(pointWkb);

        Record record = GenericRecord.create(schema);
        record.setField("id", id);
        record.setField("part", i);
        record.setField("geom", pointBuffer);
        recordList.add(record);
        id++;
      }
      recordsInDataFiles.add(recordList);
    }
    return recordsInDataFiles;
  }

  @Test
  public void testFilterTableWithSpatialPredicates() throws IOException {
    File location = tempDir.toFile();
    Table table =
        TestTables.create(
            location,
            "geom_table",
            schema,
            PartitionSpec.unpartitioned(),
            3,
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

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

    // Replace the createPoint and toGeometry calls with createBoundingBox helper calls.
    Expression expr = Expressions.stIntersects("geom", createBoundingBox(1, 1));
    validateScan(table, expr, 1);
    expr = Expressions.stIntersects("geom", createBoundingBox(0, 0));
    validateScan(table, expr, 0);
    expr = Expressions.stIntersects("geom", createBoundingBox(1.5, 1.5));
    validateScan(table, expr, 1);
    expr = Expressions.stIntersects("geom", createBoundingBox(0.5, -1.1, 1.1, 1.1));
    validateScan(table, expr, 2);
    expr = Expressions.stIntersects("geom", createBoundingBox(0, 0, 0.75, 0.75));
    validateScan(table, expr, 0);
    expr = Expressions.stIntersects("geom", createBoundingBox(0.75, 0.75, 1.25, 1.25));
    validateScan(table, expr, 1);

    expr =
        Expressions.and(
            Expressions.lessThanOrEqual("id", 10L),
            Expressions.stIntersects("geom", createBoundingBox(0.5, -1.1, 1.1, 1.1)));
    validateScan(table, expr, 1);
  }

  @Test
  public void testPartitionedGeometryTable() throws IOException {
    File location = tempDir.toFile();
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("part").build();
    Table table =
        TestTables.create(
            location,
            "test_partitioned",
            schema,
            spec,
            3,
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "parquet"));

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

    // Use createBoundingBox in spatial predicate call.
    Expression expr = Expressions.stIntersects("geom", createBoundingBox(1, 1));
    validateScan(table, expr, 1);
    expr =
        Expressions.and(
            Expressions.equal("part", 3),
            Expressions.stIntersects("geom", createBoundingBox(0.5, -1.1, 1.1, 1.1)));
    validateScan(table, expr, 1);
  }

  @SuppressWarnings("unchecked")
  private void validateScan(Table table, Expression expr, int expectedScannedFileNum)
      throws IOException {
    List<Record> results;
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).where(expr).build();
        CloseableIterator<Record> iter = reader.iterator()) {
      results = (List<Record>) IteratorUtils.toList(iter);
      if (expectedScannedFileNum > 0) {
        assertThat(results).isNotEmpty();
      }
    }
    try (CloseableIterable<FileScanTask> planFiles = table.newScan().filter(expr).planFiles();
        CloseableIterator<FileScanTask> fileScanTasks = planFiles.iterator()) {
      int numScannedFiles = IteratorUtils.toList(fileScanTasks).size();
      assertThat(numScannedFiles).isEqualTo(expectedScannedFileNum);
    }
  }

  // Helper method for a point bounding box when both min and max coordinates are equal.
  @SuppressWarnings("ParameterName")
  private static BoundingBox createBoundingBox(double x, double y) {
    return new BoundingBox(GeospatialBound.createXY(x, y), GeospatialBound.createXY(x, y));
  }

  // Helper method for a bounding box defined by min and max coordinates.
  private static BoundingBox createBoundingBox(double minX, double minY, double maxX, double maxY) {
    return new BoundingBox(
        GeospatialBound.createXY(minX, minY), GeospatialBound.createXY(maxX, maxY));
  }
}
