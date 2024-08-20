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
import java.util.stream.Collectors;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Sketch;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.stats.ThetaSketchAgg;

public class NDVSketchUtil {

  private NDVSketchUtil() {}

  public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";

  static List<Blob> generateSketches(
      SparkSession spark, Table table, Snapshot snapshot, List<String> columns) {
    Map<Integer, Sketch> columnToSketchMap = computeNDVSketches(spark, table, snapshot, columns);
    return generateBlobs(table, columns, columnToSketchMap, snapshot);
  }

  private static List<Blob> generateBlobs(
      Table table, List<String> columns, Map<Integer, Sketch> sketchMap, Snapshot snapshot) {
    Schema schema = table.schemas().get(snapshot.schemaId());
    return columns.stream()
        .map(
            columnName -> {
              Types.NestedField field = schema.findField(columnName);
              Sketch sketch = sketchMap.get(field.fieldId());
              long ndv = (long) sketch.getEstimate();
              return new Blob(
                  StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                  ImmutableList.of(field.fieldId()),
                  snapshot.snapshotId(),
                  snapshot.sequenceNumber(),
                  ByteBuffer.wrap(sketch.toByteArray()),
                  PuffinCompressionCodec.ZSTD,
                  ImmutableMap.of(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY, String.valueOf(ndv)));
            })
        .collect(Collectors.toList());
  }

  private static Map<Integer, Sketch> computeNDVSketches(
      SparkSession spark, Table table, Snapshot snapshot, List<String> columnsToBeAnalyzed) {
    Map<Integer, Sketch> sketchMap = Maps.newHashMap();
    String tableName = table.name();
    List<String> columns = ImmutableList.copyOf(columnsToBeAnalyzed);

    Column[] aggregateColumns =
        columns.stream()
            .map(
                columnName -> {
                  ThetaSketchAgg thetaSketchAgg = new ThetaSketchAgg(columnName);
                  return new Column(thetaSketchAgg.toAggregateExpression());
                })
            .toArray(Column[]::new);

    Row sketches =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SNAPSHOT_ID, snapshot.snapshotId())
            .load(tableName)
            .select(aggregateColumns)
            .first();
    Schema schema = table.schemas().get(snapshot.schemaId());
    for (int i = 0; i < columns.size(); i++) {
      Types.NestedField field = schema.findField(columns.get(i));
      sketchMap.put(field.fieldId(), CompactSketch.wrap(Memory.wrap((byte[]) sketches.get(i))));
    }
    return sketchMap;
  }
}
