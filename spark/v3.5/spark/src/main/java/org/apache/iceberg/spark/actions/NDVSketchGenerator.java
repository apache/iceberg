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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.datasketches.theta.Sketch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkValueConverter;
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

  static List<Blob> generateNDVSketchesAndBlobs(
      SparkSession spark, Table table, Snapshot snapshot, Set<String> columnsToBeAnalyzed) {
    Map<Integer, ThetaSketchJavaSerializable> columnToSketchMap =
        computeNDVSketches(spark, table, snapshot.snapshotId(), columnsToBeAnalyzed);
    return generateBlobs(table, columnsToBeAnalyzed, columnToSketchMap, snapshot);
  }

  private static List<Blob> generateBlobs(
      Table table,
      Set<String> columns,
      Map<Integer, ThetaSketchJavaSerializable> sketchMap,
      Snapshot snapshot) {
    return columns.stream()
        .map(
            columnName -> {
              Types.NestedField field = table.schema().findField(columnName);
              Sketch sketch = sketchMap.get(field.fieldId()).getSketch();
              long ndv = (long) sketch.getEstimate();
              return new Blob(
                  StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                  ImmutableList.of(field.fieldId()),
                  snapshot.snapshotId(),
                  snapshot.sequenceNumber(),
                  ByteBuffer.wrap(sketch.toByteArray()),
                  null,
                  ImmutableMap.of(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY, String.valueOf(ndv)));
            })
        .collect(Collectors.toList());
  }

  private static Map<Integer, ThetaSketchJavaSerializable> computeNDVSketches(
      SparkSession spark, Table table, long snapshotId, Set<String> columnsToBeAnalyzed) {
    Map<Integer, ThetaSketchJavaSerializable> sketchMap = Maps.newHashMap();
    String tableName = table.name();
    List<String> columns = ImmutableList.copyOf(columnsToBeAnalyzed);
    Dataset<Row> data =
        spark
            .read()
            .option(SparkReadOptions.SNAPSHOT_ID, snapshotId)
            .table(tableName)
            .select(columns.stream().map(functions::col).toArray(Column[]::new));
    Schema schema = table.schema();
    List<Types.NestedField> nestedFields =
        columns.stream().map(schema::findField).collect(Collectors.toList());
    Schema sketchSchema = new Schema(nestedFields);
    JavaPairRDD<Integer, ByteBuffer> colNameAndSketchPairRDD =
        data.javaRDD()
            .flatMap(
                row -> {
                  List<Tuple2<Integer, ByteBuffer>> columnsList =
                      Lists.newArrayListWithExpectedSize(columns.size());

                  Record record = SparkValueConverter.convert(sketchSchema, row);
                  for (int i = 0; i < record.size(); i++) {
                    columnsList.add(
                        new Tuple2<>(
                            nestedFields.get(i).fieldId(),
                            Conversions.toByteBuffer(nestedFields.get(i).type(), record.get(i))));
                  }
                  return columnsList.iterator();
                })
            .mapToPair(t -> t);
    JavaPairRDD<Integer, ThetaSketchJavaSerializable> sketches =
        colNameAndSketchPairRDD.aggregateByKey(
            new ThetaSketchJavaSerializable(),
            ThetaSketchJavaSerializable::updateSketch,
            ThetaSketchJavaSerializable::combineSketch);

    sketches.toLocalIterator().forEachRemaining(tuple -> sketchMap.put(tuple._1, tuple._2));
    return sketchMap;
  }
}
