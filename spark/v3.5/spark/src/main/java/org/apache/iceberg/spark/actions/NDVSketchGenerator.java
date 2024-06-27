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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.datasketches.theta.Sketch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import scala.Tuple2;

public class NDVSketchGenerator {

  private NDVSketchGenerator() {}

  public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";
  private static final String SNAPSHOT_ID = "snapshot-id";

  static List<Blob> generateNDVSketchesAndBlobs(
      SparkSession spark, Table table, long snapshotId, Set<String> columnsToBeAnalyzed) {
    Iterator<Tuple2<String, ThetaSketchJavaSerializable>> columnToNdvSketchPair =
        computeNDVSketches(spark, table, snapshotId, columnsToBeAnalyzed);
    Map<String, ThetaSketchJavaSerializable> sketchMap = Maps.newHashMap();
    columnToNdvSketchPair.forEachRemaining(tuple -> sketchMap.put(tuple._1, tuple._2));
    return generateBlobs(table, columnsToBeAnalyzed, sketchMap, snapshotId);
  }

  private static List<Blob> generateBlobs(
      Table table,
      Set<String> columns,
      Map<String, ThetaSketchJavaSerializable> sketchMap,
      long snapshotId) {
    return columns.stream()
        .map(
            columnName -> {
              Sketch sketch = sketchMap.get(columnName).getSketch();
              long ndv = (long) sketch.getEstimate();
              return new Blob(
                  StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                  ImmutableList.of(table.schema().findField(columnName).fieldId()),
                  snapshotId,
                  table.snapshot(snapshotId).sequenceNumber(),
                  ByteBuffer.wrap(sketch.toByteArray()),
                  null,
                  ImmutableMap.of(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY, String.valueOf(ndv)));
            })
        .collect(Collectors.toList());
  }

  static Iterator<Tuple2<String, ThetaSketchJavaSerializable>> computeNDVSketches(
      SparkSession spark, Table table, long snapshotId, Set<String> toBeAnalyzedColumns) {
    String tableName = table.name();
    List<String> columns = ImmutableList.copyOf(toBeAnalyzedColumns);
    Dataset<Row> data =
        spark
            .read()
            .option(SNAPSHOT_ID, snapshotId)
            .table(tableName)
            .select(columns.stream().map(functions::col).toArray(Column[]::new));
    Schema schema = table.schema();
    List<Types.NestedField> nestedFields =
        columns.stream().map(schema::findField).collect(Collectors.toList());
    final JavaPairRDD<String, ByteBuffer> colNameAndSketchPairRDD =
        data.javaRDD()
            .flatMap(
                row -> {
                  List<Tuple2<String, ByteBuffer>> columnsList =
                      Lists.newArrayListWithExpectedSize(columns.size());
                  for (int i = 0; i < row.size(); i++) {
                    columnsList.add(
                        new Tuple2<>(
                            columns.get(i),
                            Conversions.toByteBuffer(nestedFields.get(i).type(), row.get(i))));
                  }
                  return columnsList.iterator();
                })
            .mapToPair(t -> t);
    JavaPairRDD<String, ThetaSketchJavaSerializable> sketches =
        colNameAndSketchPairRDD.aggregateByKey(
            new ThetaSketchJavaSerializable(),
            ThetaSketchJavaSerializable::updateSketch,
            ThetaSketchJavaSerializable::combineSketch);

    return sketches.toLocalIterator();
  }
}
