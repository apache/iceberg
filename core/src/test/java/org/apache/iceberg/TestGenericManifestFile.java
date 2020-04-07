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

package org.apache.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGenericManifestFile {

  private static final FileIO FILE_IO = new TestTables.LocalFileIO();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testManifestsWithoutRowStats() throws IOException {
    File manifestListFile = temp.newFile("manifest-list.avro");
    Assert.assertTrue(manifestListFile.delete());

    Collection<String> columnNamesWithoutRowStats = ImmutableList.of(
        "manifest_path", "manifest_length", "partition_spec_id", "added_snapshot_id",
        "added_data_files_count", "existing_data_files_count", "deleted_data_files_count",
        "partitions");
    Schema schemaWithoutRowStats = ManifestFile.schema().select(columnNamesWithoutRowStats);

    OutputFile outputFile = FILE_IO.newOutputFile(manifestListFile.getCanonicalPath());
    try (FileAppender<ManifestFile> appender = Avro.write(outputFile)
        .schema(schemaWithoutRowStats)
        .named("manifest_file")
        .overwrite()
        .build()) {

      appender.add(new GenericManifestFile("path/to/manifest.avro", 1024, 1, 100L, 2, 3, 4, ImmutableList.of()));
    }

    InputFile inputFile = FILE_IO.newInputFile(manifestListFile.getCanonicalPath());
    try (CloseableIterable<ManifestFile> files = Avro.read(inputFile)
        .rename("manifest_file", GenericManifestFile.class.getName())
        .rename("partitions", GenericPartitionFieldSummary.class.getName())
        .rename("r508", GenericPartitionFieldSummary.class.getName())
        .classLoader(GenericManifestFile.class.getClassLoader())
        .project(ManifestFile.schema())
        .reuseContainers(false)
        .build()) {

      ManifestFile manifest = Iterables.getOnlyElement(files);

      Assert.assertTrue("Added files should be present", manifest.hasAddedFiles());
      Assert.assertEquals("Added files count should match", 2, (int) manifest.addedFilesCount());
      Assert.assertNull("Added rows count should be null", manifest.addedRowsCount());

      Assert.assertTrue("Existing files should be present", manifest.hasExistingFiles());
      Assert.assertEquals("Existing files count should match", 3, (int) manifest.existingFilesCount());
      Assert.assertNull("Existing rows count should be null", manifest.existingRowsCount());

      Assert.assertTrue("Deleted files should be present", manifest.hasDeletedFiles());
      Assert.assertEquals("Deleted files count should match", 4, (int) manifest.deletedFilesCount());
      Assert.assertNull("Deleted rows count should be null", manifest.deletedRowsCount());
    }
  }
}
