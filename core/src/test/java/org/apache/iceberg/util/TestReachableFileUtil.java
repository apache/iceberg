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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestReachableFileUtil {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()), optional(2, "c2", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private Table table;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    this.table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
  }

  @Test
  public void testManifestListLocations() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    List<String> manifestListPaths = ReachableFileUtil.manifestListLocations(table);
    Assert.assertEquals(manifestListPaths.size(), 2);
  }

  @Test
  public void testMetadataFileLocations() {
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1").commit();

    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    Set<String> metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, true);
    Assert.assertEquals(metadataFileLocations.size(), 4);

    metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, false);
    Assert.assertEquals(metadataFileLocations.size(), 2);
  }

  @Test
  public void testMetadataFileLocationsWithMissingFiles() {
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1").commit();

    table.newAppend().appendFile(FILE_A).commit();

    TableOperations operations = ((HasTableOperations) table).operations();
    String location = operations.current().metadataFileLocation();
    table.newAppend().appendFile(FILE_B).commit();

    // delete v3.metadata.json making v2.metadata.json and v1.metadata.json inaccessible
    table.io().deleteFile(location);

    Set<String> metadataFileLocations = ReachableFileUtil.metadataFileLocations(table, true);
    Assert.assertEquals(metadataFileLocations.size(), 2);
  }

  @Test
  public void testVersionHintWithStaticTables() {
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata metadata = ops.current();
    String metadataFileLocation = metadata.metadataFileLocation();

    StaticTableOperations staticOps = new StaticTableOperations(metadataFileLocation, table.io());
    Table staticTable = new BaseTable(staticOps, metadataFileLocation);

    String reportedVersionHintLocation = ReachableFileUtil.versionHintLocation(staticTable);
    String expectedVersionHintLocation = ops.metadataFileLocation(Util.VERSION_HINT_FILENAME);
    Assert.assertEquals(expectedVersionHintLocation, reportedVersionHintLocation);
  }

  @Test
  public void testVersionHintWithBucketNameAsLocation() {
    Table mockTable = mock(Table.class);
    when(mockTable.location()).thenReturn("s3://bucket1");
    String reportedVersionHintLocation = ReachableFileUtil.versionHintLocation(mockTable);
    String expectedVersionHintLocation = "s3://bucket1/metadata/" + Util.VERSION_HINT_FILENAME;
    Assert.assertEquals(expectedVersionHintLocation, reportedVersionHintLocation);
  }
}
