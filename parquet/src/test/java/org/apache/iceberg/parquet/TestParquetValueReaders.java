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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Optional;
import java.util.PrimitiveIterator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.junit.jupiter.api.Test;

public class TestParquetValueReaders {

  @Test
  public void positionReaderWithSequentialRows() {
    ParquetValueReader<Long> reader = ParquetValueReaders.position();

    reader.setPageSource(pageReadStore(1_000L, 4L));

    assertThat(reader.read(null)).isEqualTo(1_000L);
    assertThat(reader.read(null)).isEqualTo(1_001L);
    assertThat(reader.read(null)).isEqualTo(1_002L);
    assertThat(reader.read(null)).isEqualTo(1_003L);
  }

  @Test
  public void positionReaderWithRowIndexes() {
    ParquetValueReader<Long> reader = ParquetValueReaders.position();

    reader.setPageSource(pageReadStore(1_000L, 100L, 101L, 500L, 501L));

    assertThat(reader.read(null)).isEqualTo(1_100L);
    assertThat(reader.read(null)).isEqualTo(1_101L);
    assertThat(reader.read(null)).isEqualTo(1_500L);
    assertThat(reader.read(null)).isEqualTo(1_501L);
  }

  @Test
  public void positionReaderResetsForNewPageStore() {
    ParquetValueReader<Long> reader = ParquetValueReaders.position();

    reader.setPageSource(pageReadStore(100L, 10L, 20L));

    assertThat(reader.read(null)).isEqualTo(110L);
    assertThat(reader.read(null)).isEqualTo(120L);

    reader.setPageSource(pageReadStore(1_000L, 2L));

    assertThat(reader.read(null)).isEqualTo(1_000L);
    assertThat(reader.read(null)).isEqualTo(1_001L);
  }

  private static PageReadStore pageReadStore(long rowIndexOffset, long rowCount) {
    return new TestPageReadStore(rowCount, rowIndexOffset, null);
  }

  private static PageReadStore pageReadStore(long rowIndexOffset, long... rowIndexes) {
    return new TestPageReadStore(rowIndexes.length, rowIndexOffset, rowIndexes);
  }

  private record TestPageReadStore(long rowCount, long rowIndexOffset, long[] rowIndexes)
      implements PageReadStore {

    @Override
    public PageReader getPageReader(ColumnDescriptor descriptor) {
      throw new UnsupportedOperationException("Page readers are not used by position reader tests");
    }

    @Override
    public long getRowCount() {
      return rowCount;
    }

    @Override
    public Optional<Long> getRowIndexOffset() {
      return Optional.of(rowIndexOffset);
    }

    @Override
    public Optional<PrimitiveIterator.OfLong> getRowIndexes() {
      if (rowIndexes == null) {
        return Optional.empty();
      }

      return Optional.of(Arrays.stream(rowIndexes).iterator());
    }
  }
}
