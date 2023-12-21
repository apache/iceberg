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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestComputePartitionStatsAction extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));
  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).truncate("c2", 2).identity("c3").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testPartitionTable() {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records =
        ImmutableList.of(
            new ThreeColumnRecord(1, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    List<String> validFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("Should be 2 valid files", 4, validFiles.size());

    SparkActions actions = SparkActions.get();

    ComputePartitionStats.Result result1 = actions.computePartitionStatistics(table).execute();

    // PartitionStatsUtil.readPartitionStatsFile()
  }
}
