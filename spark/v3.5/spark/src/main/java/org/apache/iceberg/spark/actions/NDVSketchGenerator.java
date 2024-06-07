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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import scala.Tuple2;

public class NDVSketchGenerator {

  private NDVSketchGenerator() {}

  static StatisticsFile generateNDV(
      SparkSession spark, Table table, long snapshotId, String... columnsToBeAnalyzed)
      throws IOException {
    List<String> columnList;
    if (columnsToBeAnalyzed == null || columnsToBeAnalyzed.length == 0) {
      columnList =
          table.schema().columns().stream()
              .map(Types.NestedField::name)
              .collect(Collectors.toList());
    } else {
      columnList = Lists.newArrayList(columnsToBeAnalyzed);
    }
    Iterator<Tuple2<String, ThetaSketchJavaSerializable>> tuple2Iterator =
        computeNDVSketches(spark, table.name(), snapshotId, columnList);
    Map<String, ThetaSketchJavaSerializable> sketchMap = Maps.newHashMap();

    tuple2Iterator.forEachRemaining(tuple -> sketchMap.put(tuple._1, tuple._2));
    return writeToPuffin(table, columnList, sketchMap);
  }

  private static StatisticsFile writeToPuffin(
      Table table, List<String> columns, Map<String, ThetaSketchJavaSerializable> sketchMap)
      throws IOException {
    TableOperations operations = ((HasTableOperations) table).operations();
    FileIO fileIO = operations.io();
    String path = operations.metadataFileLocation(String.format("%s.stats", UUID.randomUUID()));
    OutputFile outputFile = fileIO.newOutputFile(path);
    try (PuffinWriter writer =
        Puffin.write(outputFile).createdBy("Iceberg Analyze action").build()) {
      for (String columnName : columns) {
        writer.add(
            new Blob(
                StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                ImmutableList.of(table.schema().findField(columnName).fieldId()),
                table.currentSnapshot().snapshotId(),
                table.currentSnapshot().sequenceNumber(),
                ByteBuffer.wrap(sketchMap.get(columnName).getSketch().toByteArray()),
                null,
                ImmutableMap.of()));
      }
      writer.finish();

      return new GenericStatisticsFile(
          table.currentSnapshot().snapshotId(),
          path,
          writer.fileSize(),
          writer.footerSize(),
          writer.writtenBlobsMetadata().stream()
              .map(GenericBlobMetadata::from)
              .collect(ImmutableList.toImmutableList()));
    }
  }

  static Iterator<Tuple2<String, ThetaSketchJavaSerializable>> computeNDVSketches(
      SparkSession spark, String tableName, long snapshotId, List<String> columns) {
    Dataset<Row> data =
        spark
            .read()
            .option("snapshot-id", snapshotId)
            .table(tableName)
            .select(columns.stream().map(functions::col).toArray(Column[]::new));

    final JavaPairRDD<String, String> pairs =
        data.javaRDD()
            .flatMap(
                row -> {
                  List<Tuple2<String, String>> columnsList =
                      Lists.newArrayListWithExpectedSize(columns.size());
                  for (int i = 0; i < row.size(); i++) {
                    columnsList.add(new Tuple2<>(columns.get(i), row.get(i).toString()));
                  }
                  return columnsList.iterator();
                })
            .mapToPair(t -> t);

    JavaPairRDD<String, ThetaSketchJavaSerializable> sketches =
        pairs.aggregateByKey(
            new ThetaSketchJavaSerializable(),
            Integer.parseInt(
                SQLConf.SHUFFLE_PARTITIONS().defaultValueString()), // number of partitions
            (sketch, val) -> {
              sketch.update(val);
              return sketch;
            },
            new Combine());

    return sketches.toLocalIterator();
  }

  static class Combine
      implements Function2<
          ThetaSketchJavaSerializable, ThetaSketchJavaSerializable, ThetaSketchJavaSerializable> {
    static final ThetaSketchJavaSerializable emptySketchWrapped =
        new ThetaSketchJavaSerializable(UpdateSketch.builder().build().compact());

    public ThetaSketchJavaSerializable call(
        final ThetaSketchJavaSerializable sketch1, final ThetaSketchJavaSerializable sketch2)
        throws Exception {
      if (sketch1.getSketch() == null && sketch2.getSketch() == null) {
        return emptySketchWrapped;
      }
      if (sketch1.getSketch() == null) {
        return sketch2;
      }
      if (sketch2.getSketch() == null) {
        return sketch1;
      }

      final CompactSketch compactSketch1 = sketch1.getCompactSketch();
      final CompactSketch compactSketch2 = sketch2.getCompactSketch();
      return new ThetaSketchJavaSerializable(
          new SetOperationBuilder().buildUnion().union(compactSketch1, compactSketch2));
    }
  }
}
