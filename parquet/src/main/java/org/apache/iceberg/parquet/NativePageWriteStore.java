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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

/** Buffers compressed pages of one row group until they are flushed to a native file writer. */
@SuppressWarnings("deprecation")
class NativePageWriteStore implements PageWriteStore, BloomFilterWriteStore {
  private final Map<ColumnDescriptor, ColumnPages> columns = Maps.newLinkedHashMap();

  NativePageWriteStore(BytesInputCompressor compressor, MessageType schema) {
    for (ColumnDescriptor descriptor : schema.getColumns()) {
      columns.put(descriptor, new ColumnPages(descriptor, compressor));
    }
  }

  @Override
  public PageWriter getPageWriter(ColumnDescriptor path) {
    return columns.get(path);
  }

  @Override
  public BloomFilterWriter getBloomFilterWriter(ColumnDescriptor path) {
    return columns.get(path);
  }

  @Override
  public void close() {
    columns.clear();
  }

  void flushToFileWriter(NativeParquetFileWriter writer) throws IOException {
    for (ColumnPages column : columns.values()) {
      column.flushTo(writer);
    }
  }

  private interface PageAction {
    void write(NativeParquetFileWriter writer) throws IOException;
  }

  private static class ColumnPages implements PageWriter, BloomFilterWriter {
    private final ColumnDescriptor descriptor;
    private final BytesInputCompressor compressor;
    private final List<PageAction> pages = Lists.newArrayList();
    private DictionaryPage dictionary;
    private BloomFilter bloomFilter;
    private long totalValueCount;
    private long memSize;

    ColumnPages(ColumnDescriptor descriptor, BytesInputCompressor compressor) {
      this.descriptor = descriptor;
      this.compressor = compressor;
    }

    @Override
    @SuppressWarnings("deprecation")
    public void writePage(
        BytesInput bytes,
        int valueCount,
        Statistics<?> statistics,
        Encoding rlEncoding,
        Encoding dlEncoding,
        Encoding valuesEncoding)
        throws IOException {
      writePage(bytes, valueCount, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
    }

    @Override
    public void writePage(
        BytesInput bytes,
        int valueCount,
        int rowCount,
        Statistics<?> statistics,
        Encoding rlEncoding,
        Encoding dlEncoding,
        Encoding valuesEncoding)
        throws IOException {
      int uncompressedSize = Math.toIntExact(bytes.size());
      BytesInput compressed = BytesInput.copy(compressor.compress(bytes));
      Statistics<?> stats = statistics == null ? null : statistics.copy();
      this.totalValueCount += valueCount;
      this.memSize += compressed.size();
      pages.add(
          writer ->
              writer.writeDataPage(
                  valueCount,
                  uncompressedSize,
                  compressed,
                  stats,
                  rowCount,
                  rlEncoding,
                  dlEncoding,
                  valuesEncoding));
    }

    @Override
    public void writePage(
        BytesInput bytes,
        int valueCount,
        int rowCount,
        Statistics<?> statistics,
        SizeStatistics sizeStatistics,
        GeospatialStatistics geospatialStatistics,
        Encoding rlEncoding,
        Encoding dlEncoding,
        Encoding valuesEncoding)
        throws IOException {
      GeospatialStatistics geo = geospatialStatistics == null ? null : geospatialStatistics.copy();
      SizeStatistics size = sizeStatistics == null ? null : sizeStatistics.copy();
      writePage(bytes, valueCount, rowCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
      PageAction dataPage = pages.remove(pages.size() - 1);
      pages.add(
          writer -> {
            writer.pageStatistics(size, geo);
            dataPage.write(writer);
          });
    }

    @Override
    public void writePageV2(
        int rowCount,
        int nullCount,
        int valueCount,
        BytesInput repetitionLevels,
        BytesInput definitionLevels,
        Encoding dataEncoding,
        BytesInput data,
        Statistics<?> statistics)
        throws IOException {
      boolean compress = compressor.getCodecName() != CompressionCodecName.UNCOMPRESSED;
      int uncompressedDataSize = Math.toIntExact(data.size());
      BytesInput repetition = BytesInput.copy(repetitionLevels);
      BytesInput definition = BytesInput.copy(definitionLevels);
      BytesInput values = BytesInput.copy(compress ? compressor.compress(data) : data);
      Statistics<?> stats = statistics == null ? null : statistics.copy();
      this.totalValueCount += valueCount;
      this.memSize += repetition.size() + definition.size() + values.size();
      pages.add(
          writer ->
              writer.writeDataPageV2(
                  rowCount,
                  nullCount,
                  valueCount,
                  repetition,
                  definition,
                  dataEncoding,
                  values,
                  uncompressedDataSize,
                  compress,
                  stats));
    }

    @Override
    public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException {
      Preconditions.checkState(dictionary == null, "Only one dictionary page per column chunk");
      BytesInput compressed = BytesInput.copy(compressor.compress(dictionaryPage.getBytes()));
      this.memSize += compressed.size();
      this.dictionary =
          new DictionaryPage(
              compressed,
              Math.toIntExact(dictionaryPage.getBytes().size()),
              dictionaryPage.getDictionarySize(),
              dictionaryPage.getEncoding());
    }

    @Override
    public void writePageV2(
        int rowCount,
        int nullCount,
        int valueCount,
        BytesInput repetitionLevels,
        BytesInput definitionLevels,
        Encoding dataEncoding,
        BytesInput data,
        Statistics<?> statistics,
        SizeStatistics sizeStatistics,
        GeospatialStatistics geospatialStatistics)
        throws IOException {
      GeospatialStatistics geo = geospatialStatistics == null ? null : geospatialStatistics.copy();
      SizeStatistics size = sizeStatistics == null ? null : sizeStatistics.copy();
      writePageV2(
          rowCount,
          nullCount,
          valueCount,
          repetitionLevels,
          definitionLevels,
          dataEncoding,
          data,
          statistics);
      PageAction dataPage = pages.remove(pages.size() - 1);
      pages.add(
          writer -> {
            writer.pageStatistics(size, geo);
            dataPage.write(writer);
          });
    }

    @Override
    public void close() {
      pages.clear();
    }

    @Override
    public void writeBloomFilter(BloomFilter filter) {
      this.bloomFilter = filter;
    }

    @Override
    public long getMemSize() {
      return memSize;
    }

    @Override
    public long allocatedSize() {
      return memSize;
    }

    @Override
    public String memUsageString(String prefix) {
      return prefix + " " + descriptor + " {" + memSize + " bytes}";
    }

    void flushTo(NativeParquetFileWriter writer) throws IOException {
      writer.startColumn(descriptor, totalValueCount, compressor.getCodecName());
      if (dictionary != null) {
        writer.writeDictionaryPage(dictionary);
      }

      for (PageAction page : pages) {
        page.write(writer);
      }

      if (bloomFilter != null) {
        writer.addBloomFilter(String.join(".", descriptor.getPath()), bloomFilter);
      }

      writer.endColumn();
    }
  }
}
