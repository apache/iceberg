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

package com.netflix.iceberg.hadoop;

import com.google.common.collect.Lists;
import com.netflix.iceberg.AssertHelpers;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.UpdateSchema;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.util.List;

import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;


public class TestHadoopCommits extends HadoopTableTestBase {

  @Test
  public void testCreateTable() throws Exception {
    PartitionSpec expectedSpec = PartitionSpec.builderFor(TABLE_SCHEMA)
        .bucket("data", 16)
        .build();

    Assert.assertEquals("Table schema should match schema with reassigned ids",
        TABLE_SCHEMA.asStruct(), table.schema().asStruct());
    Assert.assertEquals("Table partition spec should match with reassigned ids",
        expectedSpec, table.spec());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    Assert.assertTrue("Table location should exist",
        tableDir.exists());
    Assert.assertTrue("Should create metadata folder",
        metadataDir.exists() && metadataDir.isDirectory());
    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());
    Assert.assertTrue("Should create version hint file",
        versionHintFile.exists());
    Assert.assertEquals("Should write the current version to the hint file",
        1, readVersionHint());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    Assert.assertEquals("Table schema should match schema with reassigned ids",
        UPDATED_SCHEMA.asStruct(), table.schema().asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testSchemaUpdateComplexType() throws Exception {
    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    Types.StructType complexColumn = Types.StructType.of(
        required(0, "w", Types.IntegerType.get()),
        required(1, "x", Types.StringType.get()),
        required(2, "y", Types.BooleanType.get()),
        optional(3, "z", Types.MapType.ofOptional(
            0, 1, Types.IntegerType.get(), Types.StringType.get()
        ))
    );
    Schema updatedSchema = new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()),
        optional(3, "complex", Types.StructType.of(
            required(4, "w", Types.IntegerType.get()),
            required(5, "x", Types.StringType.get()),
            required(6, "y", Types.BooleanType.get()),
            optional(7, "z", Types.MapType.ofOptional(
                8, 9, Types.IntegerType.get(), Types.StringType.get()
            ))
        ))
    );

    table.updateSchema()
        .addColumn("complex", complexColumn)
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());
    Assert.assertEquals("Table schema should match schema with reassigned ids",
        updatedSchema.asStruct(), table.schema().asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should not create any scan tasks", 0, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testFailedCommit() throws Exception {
    // apply the change to metadata without committing
    UpdateSchema update = table.updateSchema().addColumn("n", Types.IntegerType.get());
    update.apply();

    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    version(2).createNewFile();

    AssertHelpers.assertThrows("Should fail to commit change based on v1 when v2 exists",
        CommitFailedException.class, "Version 2 already exists", update::commit);

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testStaleMetadata() throws Exception {
    Table tableCopy = TABLES.load(tableLocation);

    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    // prepare changes on the copy without committing
    UpdateSchema updateCopy = tableCopy.updateSchema()
        .addColumn("m", Types.IntegerType.get());
    updateCopy.apply();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertNotEquals("Unmodified copy should be out of date after update",
        table.schema().asStruct(), tableCopy.schema().asStruct());

    // update the table
    tableCopy.refresh();

    Assert.assertEquals("Copy should be back in sync",
        table.schema().asStruct(), tableCopy.schema().asStruct());

    AssertHelpers.assertThrows("Should fail with stale base metadata",
        CommitFailedException.class, "based on stale table metadata", updateCopy::commit);

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain 0 Avro manifest files", 0, manifests.size());
  }

  @Test
  public void testStaleVersionHint() throws Exception {
    Table stale = TABLES.load(tableLocation);

    Assert.assertTrue("Should create v1 metadata",
        version(1).exists() && version(1).isFile());
    Assert.assertFalse("Should not create v2 or newer versions",
        version(2).exists());

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    Assert.assertNotEquals("Stable table schema should not match",
        UPDATED_SCHEMA.asStruct(), stale.schema().asStruct());

    // roll the version hint back to 1
    replaceVersionHint(1);

    Table reloaded = TABLES.load(tableLocation);
    Assert.assertEquals("Updated schema for newly loaded table should match",
        UPDATED_SCHEMA.asStruct(), reloaded.schema().asStruct());

    stale.refresh();
    Assert.assertEquals("Refreshed schema for stale table should match",
        UPDATED_SCHEMA.asStruct(), reloaded.schema().asStruct());
  }

  @Test
  public void testFastAppend() throws Exception {
    // first append
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Assert.assertTrue("Should create v2 for the update",
        version(2).exists() && version(2).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        2, readVersionHint());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should scan 1 file", 1, tasks.size());

    List<File> manifests = listManifestFiles();
    Assert.assertEquals("Should contain only one Avro manifest file", 1, manifests.size());

    // second append
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Assert.assertTrue("Should create v3 for the update",
        version(3).exists() && version(3).isFile());
    Assert.assertEquals("Should write the current version to the hint file",
        3, readVersionHint());

    tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should scan 2 files", 2, tasks.size());

    Assert.assertEquals("Should contain 2 Avro manifest files",
        2, listManifestFiles().size());

    TableMetadata metadata = readMetadataVersion(3);
    Assert.assertEquals("Current snapshot should contain 2 manifests",
        2, metadata.currentSnapshot().manifests().size());
  }

  @Test
  public void testMergeAppend() throws Exception {
    testFastAppend(); // create 2 compatible manifest files that will be merged

    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    // third append
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assert.assertEquals("Should scan 3 files", 3, tasks.size());

    Assert.assertEquals("Should contain 3 Avro manifest files",
        3, listManifestFiles().size());

    TableMetadata metadata = readMetadataVersion(5);
    Assert.assertEquals("Current snapshot should contain 1 merged manifest",
        1, metadata.currentSnapshot().manifests().size());
  }
}
