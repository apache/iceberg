/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.netflix.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import java.io.File;
import java.util.List;

import static com.netflix.iceberg.types.Types.NestedField.required;

public class TableTestBase {
  // Schema passed to create tables
  static final Schema SCHEMA = new Schema(
      required(3, "id", Types.IntegerType.get()),
      required(4, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(0)
      .build();
  static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=1") // easy way to set partition data for now
      .withRecordCount(0)
      .build();
  static final DataFile FILE_C = DataFiles.builder(SPEC)
      .withPath("/path/to/data-c.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=2") // easy way to set partition data for now
      .withRecordCount(0)
      .build();
  static final DataFile FILE_D = DataFiles.builder(SPEC)
      .withPath("/path/to/data-d.parquet")
      .withFileSizeInBytes(0)
      .withPartitionPath("data_bucket=3") // easy way to set partition data for now
      .withRecordCount(0)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  File tableDir = null;
  File metadataDir = null;
  File versionHintFile = null;
  TestTables.TestTable table = null;

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");
    this.versionHintFile = new File(metadataDir, "version-hint.text");
    this.table = create(SCHEMA, SPEC);
  }

  @After
  public void cleanupTables() throws Exception {
    TestTables.clearTables();
  }

  List<File> listMetadataFiles(String ext) {
    return Lists.newArrayList(metadataDir.listFiles(
        (dir, name) -> Files.getFileExtension(name).equalsIgnoreCase(ext)));
  }

  TestTables.TestTable create(Schema schema, PartitionSpec spec) {
    return TestTables.create(tableDir, "test", schema, spec);
  }

  TestTables.TestTable load() {
    return TestTables.load(tableDir, "test");
  }

  TableMetadata readMetadata() {
    return TestTables.readMetadata("test");
  }
}
