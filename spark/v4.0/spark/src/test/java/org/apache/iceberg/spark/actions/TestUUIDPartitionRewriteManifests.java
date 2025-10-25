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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestUUIDPartitionRewriteManifests extends TestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "uuid_col", Types.UUIDType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private String tableLocation = null;

  @TempDir private Path temp;
  @TempDir private File tableDir;

  @BeforeEach
  public void setupTableLocation() throws Exception {
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testRewriteManifestsWithUUIDPartition() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("uuid_col").build();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    // Create test data with UUID partition values
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    // Write first batch of records
    Dataset<Row> df1 =
        spark.sql(
            String.format(
                "SELECT 1 as id, cast('%s' as string) as uuid_col, 'data1' as data",
                uuid1.toString()));
    df1.write().format("iceberg").mode("append").save(tableLocation);

    // Write second batch of records
    Dataset<Row> df2 =
        spark.sql(
            String.format(
                "SELECT 2 as id, cast('%s' as string) as uuid_col, 'data2' as data",
                uuid2.toString()));
    df2.write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).as("Should have 2 manifests before rewrite").hasSize(2);

    SparkActions actions = SparkActions.get();

    // This should not throw an exception with our fix
    RewriteManifests.Result result =
        actions.rewriteManifests(table).rewriteIf(manifest -> true).execute();

    assertThat(result.rewrittenManifests()).as("Action should rewrite 2 manifests").hasSize(2);
    assertThat(result.addedManifests()).as("Action should add 1 manifest").hasSize(1);

    table.refresh();

    List<ManifestFile> newManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(newManifests).as("Should have 1 manifest after rewrite").hasSize(1);

    // Verify data is still readable
    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<Row> actualRecords = resultDF.orderBy("id").collectAsList();

    assertThat(actualRecords).hasSize(2);
    assertThat(actualRecords.get(0).getInt(0)).isEqualTo(1);
    assertThat(actualRecords.get(0).getString(2)).isEqualTo("data1");
    assertThat(actualRecords.get(1).getInt(0)).isEqualTo(2);
    assertThat(actualRecords.get(1).getString(2)).isEqualTo("data2");
  }
}
