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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestRollingFileWriters<T> extends WriterTestBase<T> {

  @Parameters(name = "formatVersion = {0}, fileFormat = {1}, Partitioned = {2}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {2, FileFormat.AVRO, false},
        new Object[] {2, FileFormat.AVRO, true},
        new Object[] {2, FileFormat.PARQUET, false},
        new Object[] {2, FileFormat.PARQUET, true},
        new Object[] {2, FileFormat.ORC, false},
        new Object[] {2, FileFormat.ORC, true});
  }

  private static final int FILE_SIZE_CHECK_ROWS_DIVISOR = 1000;
  private static final long DEFAULT_FILE_SIZE = 128L * 1024 * 1024;
  private static final long SMALL_FILE_SIZE = 2L;
  private static final String PARTITION_VALUE = "aaa";

  @Parameter(index = 1)
  private FileFormat fileFormat;

  @Parameter(index = 2)
  private boolean partitioned;

  private StructLike partition = null;
  private OutputFileFactory fileFactory = null;

  protected FileFormat format() {
    return fileFormat;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    Assert.assertTrue(tableDir.delete()); // created during table creation

    this.metadataDir = new File(tableDir, "metadata");

    if (partitioned) {
      this.table = create(SCHEMA, SPEC);
      this.partition = partitionKey(table.spec(), PARTITION_VALUE);
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
      this.partition = null;
    }

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @TestTemplate
  public void testRollingDataWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingDataWriter<T> writer =
        new RollingDataWriter<>(
            writerFactory, fileFactory, table.io(), DEFAULT_FILE_SIZE, table.spec(), partition);

    writer.close();
    Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());

    writer.close();
    Assert.assertEquals("Must be no data files", 0, writer.result().dataFiles().size());
  }

  @TestTemplate
  public void testRollingDataWriterSplitData() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingDataWriter<T> writer =
        new RollingDataWriter<>(
            writerFactory, fileFactory, table.io(), SMALL_FILE_SIZE, table.spec(), partition);

    List<T> rows = Lists.newArrayListWithExpectedSize(4 * FILE_SIZE_CHECK_ROWS_DIVISOR);
    for (int index = 0; index < 4 * FILE_SIZE_CHECK_ROWS_DIVISOR; index++) {
      rows.add(toRow(index, PARTITION_VALUE));
    }

    try (RollingDataWriter<T> closableWriter = writer) {
      closableWriter.write(rows);
    }

    // call close again to ensure it is idempotent
    writer.close();

    Assert.assertEquals(4, writer.result().dataFiles().size());
  }

  @TestTemplate
  public void testRollingEqualityDeleteWriterNoRecords() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);
    RollingEqualityDeleteWriter<T> writer =
        new RollingEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), DEFAULT_FILE_SIZE, table.spec(), partition);

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());
  }

  @TestTemplate
  public void testRollingEqualityDeleteWriterSplitDeletes() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);
    RollingEqualityDeleteWriter<T> writer =
        new RollingEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), SMALL_FILE_SIZE, table.spec(), partition);

    List<T> deletes = Lists.newArrayListWithExpectedSize(4 * FILE_SIZE_CHECK_ROWS_DIVISOR);
    for (int index = 0; index < 4 * FILE_SIZE_CHECK_ROWS_DIVISOR; index++) {
      deletes.add(toRow(index, PARTITION_VALUE));
    }

    try (RollingEqualityDeleteWriter<T> closeableWriter = writer) {
      closeableWriter.write(deletes);
    }

    // call close again to ensure it is idempotent
    writer.close();

    DeleteWriteResult result = writer.result();
    Assert.assertEquals(4, result.deleteFiles().size());
    Assert.assertEquals(0, result.referencedDataFiles().size());
    Assert.assertFalse(result.referencesDataFiles());
  }

  @TestTemplate
  public void testRollingPositionDeleteWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingPositionDeleteWriter<T> writer =
        new RollingPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), DEFAULT_FILE_SIZE, table.spec(), partition);

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());

    writer.close();
    Assert.assertEquals(0, writer.result().deleteFiles().size());
    Assert.assertEquals(0, writer.result().referencedDataFiles().size());
    Assert.assertFalse(writer.result().referencesDataFiles());
  }

  @TestTemplate
  public void testRollingPositionDeleteWriterSplitDeletes() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingPositionDeleteWriter<T> writer =
        new RollingPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), SMALL_FILE_SIZE, table.spec(), partition);

    List<PositionDelete<T>> deletes =
        Lists.newArrayListWithExpectedSize(4 * FILE_SIZE_CHECK_ROWS_DIVISOR);
    for (int index = 0; index < 4 * FILE_SIZE_CHECK_ROWS_DIVISOR; index++) {
      deletes.add(positionDelete("path/to/data/file-1.parquet", index, null));
    }

    try (RollingPositionDeleteWriter<T> closeableWriter = writer) {
      closeableWriter.write(deletes);
    }

    // call close again to ensure it is idempotent
    writer.close();

    DeleteWriteResult result = writer.result();
    Assert.assertEquals(4, result.deleteFiles().size());
    Assert.assertEquals(1, result.referencedDataFiles().size());
    Assert.assertTrue(result.referencesDataFiles());
  }
}
