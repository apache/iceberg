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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.deletes.DeleteCounter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Test;

public class TestDeleteFilter extends TableTestBase {

  public TestDeleteFilter() {
    super(2);
  }

  @Test
  public void testClosePositionStreamRowDeleteMarker() throws Exception {
    // Add a data file
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newFastAppend().appendFile(dataFile).commit();

    // Add a delete file
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFile.path(), 1L));
    deletes.add(Pair.of(dataFile.path(), 2L));

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deletes);
    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    // mock records
    List<Record> records = Lists.newArrayList();
    GenericRecord record =
        GenericRecord.create(
            TypeUtil.join(table.schema(), new Schema(MetadataColumns.ROW_POSITION)));
    records.add(record.copy("id", 29, "data", "a", "_pos", 1L));
    records.add(record.copy("id", 43, "data", "b", "_pos", 2L));
    records.add(record.copy("id", 61, "data", "c", "_pos", 3L));
    records.add(record.copy("id", 89, "data", "d", "_pos", 4L));

    CheckingClosableIterable<Record> data = new CheckingClosableIterable<>(records);
    CheckingClosableIterable<Long> deletePositions =
        new CheckingClosableIterable<>(
            deletes.stream().map(Pair::second).collect(Collectors.toList()));

    CloseableIterable<Record> posDeletesIterable =
        Deletes.streamingFilter(data, row -> row.get(2, Long.class), deletePositions);

    // end iterator is always wrapped with FilterIterator
    CloseableIterable<Record> eqDeletesIterable =
        Deletes.filterDeleted(posDeletesIterable, i -> false, new DeleteCounter());
    List<Record> result = Lists.newArrayList(eqDeletesIterable.iterator());

    // as first two records deleted, expect only last two records
    List<Record> expected = Lists.newArrayList();
    expected.add(record.copy("id", 61, "data", "c", "_pos", 3L));
    expected.add(record.copy("id", 89, "data", "d", "_pos", 4L));

    Assert.assertEquals(result, expected);
    Assert.assertTrue(data.isClosed());
    Assert.assertTrue(deletePositions.isClosed());
  }

  @Test
  public void testDeleteMarkerFileClosed() throws Exception {
    // Add a data file
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newFastAppend().appendFile(dataFile).commit();

    // Add a delete file
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFile.path(), 1L));
    deletes.add(Pair.of(dataFile.path(), 2L));

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deletes);
    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();

    // mock records
    List<Record> records = Lists.newArrayList();
    GenericRecord template =
        GenericRecord.create(
            TypeUtil.join(
                table.schema(),
                new Schema(MetadataColumns.ROW_POSITION, MetadataColumns.IS_DELETED)));
    records.add(record(template, 29, "a", 1, false));
    records.add(record(template, 43, "b", 2, false));
    records.add(record(template, 61, "c", 3, false));
    records.add(record(template, 89, "d", 4, false));

    CheckingClosableIterable<Record> data = new CheckingClosableIterable<>(records);
    CheckingClosableIterable<Long> deletePositions =
        new CheckingClosableIterable<>(
            deletes.stream().map(Pair::second).collect(Collectors.toList()));

    CloseableIterable<Record> resultIterable =
        Deletes.streamingMarker(
            data, row -> row.get(2, Long.class), deletePositions, row -> row.set(3, true));

    // end iterator is always wrapped with FilterIterator
    CloseableIterable<Record> eqDeletesIterable =
        Deletes.filterDeleted(resultIterable, i -> false, new DeleteCounter());
    List<Record> result = Lists.newArrayList(eqDeletesIterable.iterator());

    // expect first two records deleted
    List<Record> expected = Lists.newArrayList();
    expected.add(record(template, 29, "a", 1, true));
    expected.add(record(template, 43, "b", 2, true));
    expected.add(record(template, 61, "c", 3, false));
    expected.add(record(template, 89, "d", 4, false));

    Assert.assertEquals(result, expected);
    Assert.assertTrue(data.isClosed());
    Assert.assertTrue(deletePositions.isClosed());
  }

  private GenericRecord record(
      GenericRecord template, int id, String data, long pos, boolean isDeleted) {
    GenericRecord copy = template.copy();
    copy.set(0, id);
    copy.set(1, data);
    copy.set(2, pos);
    copy.set(3, isDeleted);
    return copy;
  }

  private static class CheckingClosableIterable<E> implements CloseableIterable<E> {
    AtomicBoolean isClosed = new AtomicBoolean(false);
    final Iterable<E> iterable;

    CheckingClosableIterable(Iterable<E> iterable) {
      this.iterable = iterable;
    }

    public boolean isClosed() {
      return isClosed.get();
    }

    @Override
    public void close() throws IOException {
      isClosed.set(true);
    }

    @Override
    public CloseableIterator<E> iterator() {
      Iterator<E> it = iterable.iterator();
      return new CloseableIterator<E>() {

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public E next() {
          return it.next();
        }

        @Override
        public void close() throws IOException {
          isClosed.set(true);
        }
      };
    }
  }
}
