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

package org.apache.iceberg.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGenericReaderDeletes extends TableTestBase {
  public TestGenericReaderDeletes() {
    super(2 /* format v2 with delete files */);
  }

  private List<Record> records = null;
  private DataFile dataFile = null;

  @Before
  public void writeTestDataFile() throws IOException {
    this.records = Lists.newArrayList();

    // records all use IDs that are in bucket id_bucket=0
    GenericRecord record = GenericRecord.create(table.schema());
    records.add(record.copy(ImmutableMap.of("id", 29, "data", "a")));
    records.add(record.copy(ImmutableMap.of("id", 43, "data", "b")));
    records.add(record.copy(ImmutableMap.of("id", 61, "data", "c")));
    records.add(record.copy(ImmutableMap.of("id", 89, "data", "d")));
    records.add(record.copy(ImmutableMap.of("id", 100, "data", "e")));
    records.add(record.copy(ImmutableMap.of("id", 121, "data", "f")));
    records.add(record.copy(ImmutableMap.of("id", 122, "data", "g")));

    OutputFile out = Files.localOutput(temp.newFile());
    FileAppender<Record> writer = Parquet.write(out)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .schema(table.schema())
        .overwrite()
        .build();

    try (Closeable toClose = writer) {
      writer.addAll(records);
    }

    this.dataFile = DataFiles.builder(table.spec())
        .withFormat(FileFormat.PARQUET)
        .withPath(out.location())
        .withPartition(Row.of(0))
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  @Test
  public void testEqualityDeletes() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    OutputFile out = Files.localOutput(temp.newFile());
    Schema deleteRowSchema = table.schema().select("data");
    EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(out)
        .forTable(table)
        .rowSchema(deleteRowSchema)
        .withPartition(Row.of(0))
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .equalityFieldIds(deleteRowSchema.findField("data").fieldId())
        .buildEqualityWriter();

    try (Closeable toClose = writer) {
      Record delete = GenericRecord.create(deleteRowSchema);
      writer.delete(delete.copy(ImmutableMap.of("data", "a"))); // id = 29
      writer.delete(delete.copy(ImmutableMap.of("data", "d"))); // id = 89
      writer.delete(delete.copy(ImmutableMap.of("data", "g"))); // id = 122
    }

    DeleteFile posDeletes = writer.toDeleteFile();
    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 122);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testPositionDeletes() throws IOException {
    table.newAppend()
        .appendFile(dataFile)
        .commit();

    OutputFile out = Files.localOutput(temp.newFile());
    PositionDeleteWriter<?> writer = Parquet.writeDeletes(out)
        .forTable(table)
        .withPartition(Row.of(0))
        .overwrite()
        .buildPositionWriter();

    try (Closeable toClose = writer) {
      writer.delete(dataFile.path(), 0); // id = 29
      writer.delete(dataFile.path(), 3); // id = 89
      writer.delete(dataFile.path(), 6); // id = 122
    }

    DeleteFile posDeletes = writer.toDeleteFile();
    table.newRowDelta()
        .addDeletes(posDeletes)
        .commit();

    StructLikeSet expected = rowSetWithoutIds(29, 89, 122);
    StructLikeSet actual = rowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  private static StructLikeSet rowSet(Table table) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  private StructLikeSet rowSetWithoutIds(int... idsToRemove) {
    Set<Integer> deletedIds = Sets.newHashSet(ArrayUtil.toIntList(idsToRemove));
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.stream()
        .filter(row -> !deletedIds.contains(row.getField("id")))
        .forEach(set::add);
    return set;
  }
}
