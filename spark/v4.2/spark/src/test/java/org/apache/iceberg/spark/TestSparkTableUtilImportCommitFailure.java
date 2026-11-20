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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkTableUtilImportCommitFailure extends TestBase {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c2", Types.StringType.get()),
          Types.NestedField.optional(3, "c3", Types.StringType.get()));

  @TempDir private Path temp;
  @TempDir private File tableDir;

  @Test
  public void testImportKeepsManifestsOnCommitStateUnknown() {
    String tableLocation = tableDir.toURI().toString();
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.builderFor(SCHEMA).identity("c3").build(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"),
            tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "BBBB"));
    String parquetTableLocation = temp.resolve("parquet_table").toUri().toString();

    try {
      spark
          .createDataFrame(records, ThreeColumnRecord.class)
          .select("c1", "c2", "c3")
          .write()
          .format("parquet")
          .mode("overwrite")
          .option("path", parquetTableLocation)
          .partitionBy("c3")
          .saveAsTable("parquet_table");

      // the append lands, then the catalog response is lost
      AppendFiles append = table.newAppend();
      AppendFiles spyAppend = spy(append);
      doAnswer(
              invocation -> {
                append.commit();
                throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
              })
          .when(spyAppend)
          .commit();
      Table spyTable = spy(table);
      when(spyTable.newAppend()).thenReturn(spyAppend);

      String stagingDir = temp.resolve("staging-dir").toString();
      assertThatThrownBy(
              () ->
                  SparkTableUtil.importSparkTable(
                      spark, new TableIdentifier("parquet_table"), spyTable, stagingDir))
          .isInstanceOf(CommitStateUnknownException.class)
          .hasMessageContaining("Datacenter on Fire");

      table.refresh();
      List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
      assertThat(manifests).isNotEmpty();
      for (ManifestFile manifest : manifests) {
        assertThat(table.io().newInputFile(manifest.path()).exists())
            .as("Manifest referenced by the committed snapshot must exist: %s", manifest.path())
            .isTrue();
      }

      Dataset<Row> result = spark.read().format("iceberg").load(tableLocation);
      assertThat(result.sort("c1").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList())
          .isEqualTo(records);
    } finally {
      spark.sql("DROP TABLE IF EXISTS parquet_table");
    }
  }
}
