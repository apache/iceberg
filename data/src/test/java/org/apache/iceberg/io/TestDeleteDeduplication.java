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
package org.apache.iceberg.io;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test delete deduplication within a checkpoint.
 *
 * <p>This test verifies that the BaseEqualityDeltaWriter correctly deduplicates delete operations
 * within a single checkpoint, which is critical for Flink CDC scenarios where the same key might be
 * deleted multiple times during high-frequency updates.
 */
public class TestDeleteDeduplication extends TableTestBase {

  private static final int FORMAT_V2 = 2;
  private FileFormat format = FileFormat.PARQUET;
  private OutputFileFactory fileFactory;
  private FileAppenderFactory<Record> appenderFactory;

  public TestDeleteDeduplication() {
    super(FORMAT_V2);
  }

  @Override
  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());
    this.metadataDir = new File(tableDir, "metadata");
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
    // Set equality field IDs to field 0 (id field) for delete operations
    // Create delete schema containing only the ID field
    Schema deleteSchema = table.schema().select("id");
    this.appenderFactory = new GenericAppenderFactory(
        table.schema(),
        PartitionSpec.unpartitioned(),
        new int[] {table.schema().findField("id").fieldId()},  // equality field IDs
        deleteSchema,  // eqDeleteRowSchema
        null);  // posDeleteRowSchema
  }

  @Test
  public void testDeleteKeyDeduplication() throws IOException {
    TestEqualityDeltaWriter writer =
        new TestEqualityDeltaWriter(
            null, // unpartitioned
            table.spec(),
            format,
            appenderFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            ImmutableList.of(table.schema().findField("id").fieldId())); // equality field: id

    try {
      // Create delete schema for keys (only id field)
      Schema deleteSchema = table.schema().select("id");

      // Simulate Flink CDC scenario: same key deleted multiple times
      Record key1 = GenericRecord.create(deleteSchema);
      key1.setField("id", 1);

      Record key2 = GenericRecord.create(deleteSchema);
      key2.setField("id", 2);

      // Delete key1 three times (simulating UPDATE events on same record)
      writer.deleteKey(key1);
      writer.deleteKey(key1); // Duplicate - should be deduplicated
      writer.deleteKey(key1); // Duplicate - should be deduplicated

      // Delete key2 twice
      writer.deleteKey(key2);
      writer.deleteKey(key2); // Duplicate - should be deduplicated

      // Complete the writer
      WriteResult result = writer.complete();

      // Verify: Should only have 2 delete records (one per unique key), not 5
      List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
      Assert.assertFalse("Should have delete files", deleteFiles.isEmpty());

      // Count total delete records
      long totalDeleteRecords =
          deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

      // Should have exactly 2 delete records (key1 and key2), not 5
      Assert.assertEquals(
          "Delete records should be deduplicated within checkpoint", 2L, totalDeleteRecords);

    } finally {
      writer.close();
    }
  }

  @Test
  public void testDeleteRowDeduplication() throws IOException {
    TestEqualityDeltaWriter writer =
        new TestEqualityDeltaWriter(
            null,
            table.spec(),
            format,
            appenderFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            ImmutableList.of(0));

    try {
      // Create full row records
      Record row1 = GenericRecord.create(table.schema());
      row1.setField("id", 1);
      row1.setField("data", "data1");

      Record row2 = GenericRecord.create(table.schema());
      row2.setField("id", 2);
      row2.setField("data", "data2");

      // Delete same rows multiple times
      writer.delete(row1);
      writer.delete(row1); // Duplicate
      writer.delete(row1); // Duplicate

      writer.delete(row2);
      writer.delete(row2); // Duplicate

      WriteResult result = writer.complete();

      // Verify deduplication
      List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
      long totalDeleteRecords =
          deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

      // Should have exactly 2 delete records, not 5
      Assert.assertEquals(
          "Delete rows should be deduplicated within checkpoint", 2L, totalDeleteRecords);

    } finally {
      writer.close();
    }
  }

  @Test
  public void testNoDuplicatesScenario() throws IOException {
    TestEqualityDeltaWriter writer =
        new TestEqualityDeltaWriter(
            null,
            table.spec(),
            format,
            appenderFactory,
            fileFactory,
            table.io(),
            Long.MAX_VALUE,
            table.schema(),
            ImmutableList.of(0));

    try {
      // Create delete schema for keys (only id field)
      Schema deleteSchema = table.schema().select("id");

      // All different keys - no deduplication should happen
      for (int i = 0; i < 100; i++) {
        Record key = GenericRecord.create(deleteSchema);
        key.setField("id", i);
        writer.deleteKey(key);
      }

      WriteResult result = writer.complete();

      List<DeleteFile> deleteFiles = Lists.newArrayList(result.deleteFiles());
      long totalDeleteRecords =
          deleteFiles.stream().mapToLong(DeleteFile::recordCount).sum();

      // Should have all 100 records since they're all unique
      Assert.assertEquals("All unique deletes should be preserved", 100L, totalDeleteRecords);

    } finally {
      writer.close();
    }
  }

  /** Test helper class to expose BaseEqualityDeltaWriter for testing. */
  private static class TestEqualityDeltaWriter extends BaseTaskWriter<Record> {

    private final List<Integer> equalityFieldIds;
    private TestDeltaWriter deltaWriter;

    TestEqualityDeltaWriter(
        StructLike partition,
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema,
        List<Integer> equalityFieldIds) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.equalityFieldIds = equalityFieldIds;

      Schema deleteSchema =
          org.apache.iceberg.types.TypeUtil.select(
              schema,
              org.apache.iceberg.relocated.com.google.common.collect.Sets.newHashSet(
                  equalityFieldIds));
      this.deltaWriter = new TestDeltaWriter(partition, schema, deleteSchema);
    }

    @Override
    public void write(Record record) throws IOException {
      deltaWriter.write(record);
    }

    public void deleteKey(Record key) throws IOException {
      deltaWriter.deleteKey(key);
    }

    public void delete(Record row) throws IOException {
      deltaWriter.delete(row);
    }

    @Override
    public void close() throws IOException {
      if (deltaWriter != null) {
        deltaWriter.close();
      }
    }

    private class TestDeltaWriter extends BaseEqualityDeltaWriter {
      TestDeltaWriter(StructLike partition, Schema schema, Schema deleteSchema) {
        super(partition, schema, deleteSchema);
      }

      @Override
      protected StructLike asStructLike(Record data) {
        return data;
      }

      @Override
      protected StructLike asStructLikeKey(Record key) {
        return key;
      }
    }
  }
}
