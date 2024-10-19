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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.ExpressionColumnNode;
import org.apache.spark.sql.stats.ThetaSketchAgg;

public class NDVSketchUtil {

  private NDVSketchUtil() {}

  public static final String APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY = "ndv";

  static List<Blob> generateBlobs(
      SparkSession spark, Table table, Snapshot snapshot, List<String> columns) {
    Row sketches = computeNDVSketches(spark, table, snapshot, columns);
    Schema schema = table.schemas().get(snapshot.schemaId());
    List<Blob> blobs = Lists.newArrayList();
    for (int i = 0; i < columns.size(); i++) {
      Types.NestedField field = schema.findField(columns.get(i));
      Sketch sketch = CompactSketch.wrap(Memory.wrap((byte[]) sketches.get(i)));
      blobs.add(toBlob(field, sketch, snapshot));
    }
    return blobs;
  }

  private static Blob toBlob(Types.NestedField field, Sketch sketch, Snapshot snapshot) {
    return new Blob(
        StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
        ImmutableList.of(field.fieldId()),
        snapshot.snapshotId(),
        snapshot.sequenceNumber(),
        ByteBuffer.wrap(sketch.toByteArray()),
        PuffinCompressionCodec.ZSTD,
        ImmutableMap.of(
            APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY,
            String.valueOf((long) sketch.getEstimate())));
  }

  private static Row computeNDVSketches(
      SparkSession spark, Table table, Snapshot snapshot, List<String> colNames) {
    Dataset<Row> inputDF = SparkTableUtil.loadTable(spark, table, snapshot.snapshotId());
    return inputDF.select(toAggColumns(colNames)).first();
  }

  private static Column[] toAggColumns(List<String> colNames) {
    return colNames.stream().map(NDVSketchUtil::toAggColumn).toArray(Column[]::new);
  }

  private static Column toAggColumn(String colName) {
    ThetaSketchAgg agg = new ThetaSketchAgg(colName);
    return new Column(ExpressionColumnNode.apply(agg.toAggregateExpression()));
  }
}
