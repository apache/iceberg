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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestReader extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestManifestReader(int formatVersion) {
    super(formatVersion);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testManifestReaderWithEmptyInheritableMetadata() throws IOException {
    ManifestFile manifest = writeManifest("manifest.avro", manifestEntry(Status.EXISTING, 1000L, FILE_A));
    try (ManifestReader reader = ManifestReader.read(FILE_IO.newInputFile(manifest.path()))) {
      ManifestEntry entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(Status.EXISTING, entry.status());
      Assert.assertEquals(FILE_A.path(), entry.file().path());
      Assert.assertEquals(1000L, (long) entry.snapshotId());
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testInvalidUsage() throws IOException {
    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    try (ManifestReader reader = ManifestReader.read(FILE_IO.newInputFile(manifest.path()))) {
      AssertHelpers.assertThrows(
          "Should not be possible to read manifest without explicit snapshot ids and inheritable metadata",
          IllegalArgumentException.class, "must have explicit snapshot ids",
          () -> Iterables.getOnlyElement(reader.entries()));
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testManifestReaderWithPartitionMetadata() throws IOException {
    ManifestFile manifest = writeManifest("manifest.avro", manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader reader = ManifestReader.read(FILE_IO.newInputFile(manifest.path()))) {
      ManifestEntry entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields = ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(1, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("data_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testManifestReaderWithUpdatedPartitionMetadataForV1Table() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(table.schema())
        .bucket("id", 8)
        .bucket("data", 16)
        .build();
    table.ops().commit(table.ops().current(), table.ops().current().updatePartitionSpec(spec));

    ManifestFile manifest = writeManifest("manifest.avro", manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader reader = ManifestReader.read(FILE_IO.newInputFile(manifest.path()))) {
      ManifestEntry entry = Iterables.getOnlyElement(reader.entries());
      Assert.assertEquals(123L, (long) entry.snapshotId());

      List<Types.NestedField> fields = ((PartitionData) entry.file().partition()).getPartitionType().fields();
      Assert.assertEquals(2, fields.size());
      Assert.assertEquals(1000, fields.get(0).fieldId());
      Assert.assertEquals("id_bucket", fields.get(0).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(0).type());

      Assert.assertEquals(1001, fields.get(1).fieldId());
      Assert.assertEquals("data_bucket", fields.get(1).name());
      Assert.assertEquals(Types.IntegerType.get(), fields.get(1).type());
    }
  }

}
