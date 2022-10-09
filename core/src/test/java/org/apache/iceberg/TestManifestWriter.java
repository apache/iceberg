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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestWriter extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestManifestWriter(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestStats() throws IOException {
    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, newFile(10)),
            manifestEntry(Status.ADDED, null, newFile(20)),
            manifestEntry(Status.ADDED, null, newFile(5)),
            manifestEntry(Status.ADDED, null, newFile(5)),
            manifestEntry(Status.EXISTING, null, newFile(15)),
            manifestEntry(Status.EXISTING, null, newFile(10)),
            manifestEntry(Status.EXISTING, null, newFile(1)),
            manifestEntry(Status.DELETED, null, newFile(5)),
            manifestEntry(Status.DELETED, null, newFile(2)));

    Assert.assertTrue("Added files should be present", manifest.hasAddedFiles());
    Assert.assertEquals("Added files count should match", 4, (int) manifest.addedFilesCount());
    Assert.assertEquals("Added rows count should match", 40L, (long) manifest.addedRowsCount());

    Assert.assertTrue("Existing files should be present", manifest.hasExistingFiles());
    Assert.assertEquals(
        "Existing files count should match", 3, (int) manifest.existingFilesCount());
    Assert.assertEquals(
        "Existing rows count should match", 26L, (long) manifest.existingRowsCount());

    Assert.assertTrue("Deleted files should be present", manifest.hasDeletedFiles());
    Assert.assertEquals("Deleted files count should match", 2, (int) manifest.deletedFilesCount());
    Assert.assertEquals("Deleted rows count should match", 7L, (long) manifest.deletedRowsCount());
  }

  @Test
  public void testManifestPartitionStats() throws IOException {
    ManifestFile manifest =
        writeManifest(
            "manifest.avro",
            manifestEntry(Status.ADDED, null, newFile(10, TestHelpers.Row.of(1))),
            manifestEntry(Status.EXISTING, null, newFile(15, TestHelpers.Row.of(2))),
            manifestEntry(Status.DELETED, null, newFile(2, TestHelpers.Row.of(3))));

    List<ManifestFile.PartitionFieldSummary> partitions = manifest.partitions();
    Assert.assertEquals("Partition field summaries count should match", 1, partitions.size());
    ManifestFile.PartitionFieldSummary partitionFieldSummary = partitions.get(0);
    Assert.assertFalse("contains_null should be false", partitionFieldSummary.containsNull());
    Assert.assertFalse("contains_nan should be false", partitionFieldSummary.containsNaN());
    Assert.assertEquals(
        "Lower bound should match",
        Integer.valueOf(1),
        Conversions.fromByteBuffer(Types.IntegerType.get(), partitionFieldSummary.lowerBound()));
    Assert.assertEquals(
        "Upper bound should match",
        Integer.valueOf(3),
        Conversions.fromByteBuffer(Types.IntegerType.get(), partitionFieldSummary.upperBound()));
  }

  @Test
  public void testWriteManifestWithSequenceNumber() throws IOException {
    Assume.assumeTrue("sequence number is only valid for format version > 1", formatVersion > 1);
    File manifestFile = temp.newFile("manifest.avro");
    Assert.assertTrue(manifestFile.delete());
    OutputFile outputFile = table.ops().io().newOutputFile(manifestFile.getCanonicalPath());
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(formatVersion, table.spec(), outputFile, 1L);
    writer.add(newFile(10, TestHelpers.Row.of(1)), 1000L);
    writer.close();
    ManifestFile manifest = writer.toManifestFile();
    Assert.assertEquals("Manifest should have no sequence number", -1L, manifest.sequenceNumber());
    ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifest, table.io());
    for (ManifestEntry<DataFile> entry : manifestReader.entries()) {
      Assert.assertEquals(
          "Custom data sequence number should be used for all manifest entries",
          1000L,
          (long) entry.dataSequenceNumber());
      Assert.assertEquals(
          "Custom sequence number should be used for all manifest entries",
          1000L,
          (long) entry.sequenceNumber());
    }
  }

  private DataFile newFile(long recordCount) {
    return newFile(recordCount, null);
  }

  private DataFile newFile(long recordCount, StructLike partition) {
    String fileName = UUID.randomUUID().toString();
    DataFiles.Builder builder =
        DataFiles.builder(SPEC)
            .withPath("data_bucket=0/" + fileName + ".parquet")
            .withFileSizeInBytes(1024)
            .withRecordCount(recordCount);
    if (partition != null) {
      builder.withPartition(partition);
    }
    return builder.build();
  }
}
