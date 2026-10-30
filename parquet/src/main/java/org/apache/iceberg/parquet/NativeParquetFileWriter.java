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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.parquet.metadata.BlockMetadata;
import org.apache.iceberg.parquet.metadata.ColumnChunkExtras;
import org.apache.iceberg.parquet.metadata.ColumnChunkMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geospatial.BoundingBox;
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics;
import org.apache.parquet.column.statistics.geospatial.GeospatialTypes;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.InternalColumnEncryptionSetup;
import org.apache.parquet.crypto.InternalFileEncryptor;
import org.apache.parquet.crypto.ModuleCipherFactory;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.SortingColumn;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Native Parquet file writer using Iceberg OutputFile instead of Hadoop FileSystem.
 *
 * <p>Replacement for org.apache.parquet.hadoop.ParquetFileWriter.
 */
@SuppressWarnings("deprecation")
class NativeParquetFileWriter implements AutoCloseable {
  private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};
  private static final byte[] ENCRYPTED_MAGIC = new byte[] {'P', 'A', 'R', 'E'};
  private static final ParquetMetadataConverter CONVERTER = new ParquetMetadataConverter();

  private static final LogicalTypeAnnotationVisitor<LogicalType> LOGICAL_TYPES =
      new LogicalTypeVisitor();

  private enum State {
    STARTED,
    BLOCK,
    COLUMN,
    ENDED
  }

  private final PositionOutputStream stream;
  private final MessageType schema;
  private final CompressionCodecName defaultCodec;
  private final long rowGroupSize;
  private final int maxPaddingSize;
  private final boolean pageChecksums;
  private final InternalFileEncryptor fileEncryptor;
  private final List<RowGroup> rowGroups = Lists.newArrayList();
  private final List<PendingIndexes> pendingIndexes = Lists.newArrayList();
  private final Map<String, BloomFilter> bloomFilters = Maps.newHashMap();

  private org.apache.iceberg.parquet.metadata.ParquetMetadata footer;
  private State state = State.STARTED;
  private boolean aborted = false;

  // current row group
  private long blockRowCount;
  private long blockStart;
  private List<ColumnChunk> blockColumns;
  private long blockUncompressedSize;
  private long blockCompressedSize;

  // current column chunk
  private ColumnDescriptor columnDescriptor;
  private CompressionCodecName columnCodec;
  private long columnValueCount;
  private long columnStart;
  private long columnDictionaryOffset;
  private long columnFirstDataPageOffset;
  private long columnUncompressedSize;
  private long columnCompressedSize;
  private Set<Encoding> columnEncodings;
  private EncodingStats.Builder columnEncodingStats;
  private Statistics<?> columnStatistics;
  private boolean columnStatisticsValid;
  private GeospatialStatistics columnGeospatialStatistics;
  private SizeStatistics columnSizeStatistics;
  private SizeStatistics nextPageSizeStatistics;
  private ColumnIndexBuilder columnIndexBuilder;
  private OffsetIndexBuilder offsetIndexBuilder;
  private int columnOrdinal;
  private int pageOrdinal;
  private BlockCipher.Encryptor columnHeaderEncryptor;
  private BlockCipher.Encryptor columnPageEncryptor;

  private NativeParquetFileWriter(
      PositionOutputStream stream,
      MessageType schema,
      CompressionCodecName defaultCodec,
      long rowGroupSize,
      int maxPaddingSize,
      boolean pageChecksums,
      InternalFileEncryptor fileEncryptor) {
    this.fileEncryptor = fileEncryptor;
    this.stream = stream;
    this.schema = schema;
    this.defaultCodec = defaultCodec;
    this.rowGroupSize = rowGroupSize;
    this.maxPaddingSize = maxPaddingSize;
    this.pageChecksums = pageChecksums;
  }

  static NativeParquetFileWriter create(
      OutputFile file, MessageType schema, CompressionCodecName codec) throws IOException {
    return create(file, schema, codec, ParquetFileWriter.Mode.CREATE, 0, 0, false, null);
  }

  static NativeParquetFileWriter create(
      OutputFile file,
      MessageType schema,
      CompressionCodecName codec,
      ParquetFileWriter.Mode mode,
      long rowGroupSize,
      int maxPaddingSize,
      boolean pageChecksums,
      InternalFileEncryptor fileEncryptor)
      throws IOException {
    PositionOutputStream stream =
        mode == ParquetFileWriter.Mode.OVERWRITE ? file.createOrOverwrite() : file.create();

    try {
      stream.write(
          fileEncryptor != null && fileEncryptor.isFooterEncrypted() ? ENCRYPTED_MAGIC : MAGIC);
      return new NativeParquetFileWriter(
          stream, schema, codec, rowGroupSize, maxPaddingSize, pageChecksums, fileEncryptor);
    } catch (IOException | RuntimeException e) {
      stream.close();
      throw e;
    }
  }

  long position() throws IOException {
    return stream.getPos();
  }

  /** Returns the footer metadata; only available after {@link #end}. */
  org.apache.iceberg.parquet.metadata.ParquetMetadata footer() {
    Preconditions.checkState(footer != null, "Footer is not available before end()");
    return footer;
  }

  /** Starts a row group of the given row count, padding to the row group size if configured. */
  void startBlock(long rowCount) throws IOException {
    Preconditions.checkState(state == State.STARTED, "Cannot start a block in state %s", state);
    padToRowGroupBoundary();
    this.state = State.BLOCK;
    this.blockRowCount = rowCount;
    this.blockStart = stream.getPos();
    this.blockColumns = Lists.newArrayList();
    this.blockUncompressedSize = 0;
    this.blockCompressedSize = 0;
  }

  void startColumn(ColumnDescriptor descriptor, long valueCount) throws IOException {
    startColumn(descriptor, valueCount, defaultCodec);
  }

  void startColumn(ColumnDescriptor descriptor, long valueCount, CompressionCodecName codec)
      throws IOException {
    Preconditions.checkState(state == State.BLOCK, "Cannot start a column in state %s", state);
    this.state = State.COLUMN;
    this.columnDescriptor = descriptor;
    this.columnCodec = codec;
    this.columnValueCount = valueCount;
    this.columnStart = stream.getPos();
    this.columnDictionaryOffset = -1;
    this.columnFirstDataPageOffset = -1;
    this.columnUncompressedSize = 0;
    this.columnCompressedSize = 0;
    this.columnEncodings = Sets.newHashSet();
    this.columnEncodingStats = new EncodingStats.Builder();
    this.columnStatistics = null;
    this.columnStatisticsValid = true;
    this.columnGeospatialStatistics = null;
    this.nextPageSizeStatistics = null;
    this.columnSizeStatistics =
        SizeStatistics.newBuilder(
                descriptor.getPrimitiveType(),
                descriptor.getMaxRepetitionLevel(),
                descriptor.getMaxDefinitionLevel())
            .build();
    this.columnIndexBuilder =
        ColumnIndexBuilder.getBuilder(descriptor.getPrimitiveType(), Integer.MAX_VALUE);
    this.offsetIndexBuilder = OffsetIndexBuilder.getBuilder();
    this.columnOrdinal = columnOrdinalOf(descriptor);
    this.pageOrdinal = -1;
    this.columnHeaderEncryptor = null;
    this.columnPageEncryptor = null;
    if (fileEncryptor != null) {
      InternalColumnEncryptionSetup setup =
          fileEncryptor.getColumnSetup(ColumnPath.get(descriptor.getPath()), true, columnOrdinal);
      if (setup.isEncrypted()) {
        this.columnHeaderEncryptor = setup.getMetaDataEncryptor();
        this.columnPageEncryptor = setup.getDataEncryptor();
      }
    }
  }

  private int columnOrdinalOf(ColumnDescriptor descriptor) {
    List<ColumnDescriptor> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      if (Arrays.equals(columns.get(i).getPath(), descriptor.getPath())) {
        return i;
      }
    }

    throw new IllegalArgumentException("Column not in schema: " + descriptor);
  }

  private byte[] moduleAad(ModuleType type, int page) {
    return AesCipher.createModuleAAD(
        fileEncryptor.getFileAAD(), type, rowGroups.size(), columnOrdinal, page);
  }

  private BytesInput encryptPage(BytesInput bytes, ModuleType type, int page) throws IOException {
    if (columnPageEncryptor == null) {
      return bytes;
    }

    return BytesInput.from(columnPageEncryptor.encrypt(bytes.toByteArray(), moduleAad(type, page)));
  }

  /** Writes a dictionary page whose bytes are already compressed with the column codec. */
  void writeDictionaryPage(DictionaryPage page) throws IOException {
    Preconditions.checkState(state == State.COLUMN, "Cannot write a page in state %s", state);
    Preconditions.checkState(columnDictionaryOffset < 0, "Only one dictionary page is allowed");
    BytesInput dictionaryBytes = encryptPage(page.getBytes(), ModuleType.DictionaryPage, -1);

    columnDictionaryOffset = stream.getPos();
    DictionaryPageHeader dictionaryHeader =
        new DictionaryPageHeader(
            page.getDictionarySize(), CONVERTER.getEncoding(page.getEncoding()));
    PageHeader header =
        new PageHeader(
            PageType.DICTIONARY_PAGE, page.getUncompressedSize(), (int) dictionaryBytes.size());
    header.setDictionary_page_header(dictionaryHeader);
    setCrc(header, dictionaryBytes);

    long start = stream.getPos();
    writeHeader(header, ModuleType.DictionaryPageHeader, -1);
    long headerSize = stream.getPos() - start;
    dictionaryBytes.writeAllTo(stream);

    columnUncompressedSize += headerSize + page.getUncompressedSize();
    columnCompressedSize += headerSize + dictionaryBytes.size();
    columnEncodings.add(page.getEncoding());
    columnEncodingStats.addDictEncoding(page.getEncoding());
  }

  /** Writes a v1 data page whose bytes are already compressed with the column codec. */
  void writeDataPage(
      int valueCount,
      int uncompressedPageSize,
      BytesInput bytes,
      Statistics<?> statistics,
      long rowCount,
      Encoding rlEncoding,
      Encoding dlEncoding,
      Encoding valuesEncoding)
      throws IOException {
    Preconditions.checkState(state == State.COLUMN, "Cannot write a page in state %s", state);
    pageOrdinal++;
    BytesInput pageBytes = encryptPage(bytes, ModuleType.DataPage, pageOrdinal);
    long compressedSize = pageBytes.size();
    Preconditions.checkArgument(
        compressedSize <= Integer.MAX_VALUE, "Page too large: %s bytes", compressedSize);

    DataPageHeader dataHeader =
        new DataPageHeader(
            valueCount,
            CONVERTER.getEncoding(valuesEncoding),
            CONVERTER.getEncoding(dlEncoding),
            CONVERTER.getEncoding(rlEncoding));
    PageHeader header =
        new PageHeader(PageType.DATA_PAGE, uncompressedPageSize, (int) compressedSize);
    header.setData_page_header(dataHeader);
    setCrc(header, pageBytes);

    long pageStart = stream.getPos();
    writeHeader(header, ModuleType.DataPageHeader, pageOrdinal);
    long headerSize = stream.getPos() - pageStart;
    pageBytes.writeAllTo(stream);

    recordDataPage(
        pageStart, headerSize, uncompressedPageSize, compressedSize, rowCount, statistics);
    columnEncodings.add(rlEncoding);
    columnEncodings.add(dlEncoding);
    columnEncodings.add(valuesEncoding);
    columnEncodingStats.addDataEncoding(valuesEncoding);
  }

  /** Writes a v2 data page; the data bytes are compressed if {@code compressed} is true. */
  void writeDataPageV2(
      int rowCount,
      int nullCount,
      int valueCount,
      BytesInput repetitionLevels,
      BytesInput definitionLevels,
      Encoding dataEncoding,
      BytesInput data,
      int uncompressedDataSize,
      boolean compressed,
      Statistics<?> statistics)
      throws IOException {
    Preconditions.checkState(state == State.COLUMN, "Cannot write a page in state %s", state);
    pageOrdinal++;
    BytesInput dataBytes = encryptPage(data, ModuleType.DataPage, pageOrdinal);
    long levelsSize = repetitionLevels.size() + definitionLevels.size();
    long uncompressedSize = levelsSize + uncompressedDataSize;
    long compressedSize = levelsSize + dataBytes.size();
    Preconditions.checkArgument(
        compressedSize <= Integer.MAX_VALUE, "Page too large: %s bytes", compressedSize);

    DataPageHeaderV2 dataHeader =
        new DataPageHeaderV2(
            valueCount,
            nullCount,
            rowCount,
            CONVERTER.getEncoding(dataEncoding),
            (int) definitionLevels.size(),
            (int) repetitionLevels.size());
    dataHeader.setIs_compressed(compressed);
    PageHeader header =
        new PageHeader(PageType.DATA_PAGE_V2, (int) uncompressedSize, (int) compressedSize);
    header.setData_page_header_v2(dataHeader);
    setCrc(header, BytesInput.concat(repetitionLevels, definitionLevels, dataBytes));

    long pageStart = stream.getPos();
    writeHeader(header, ModuleType.DataPageHeader, pageOrdinal);
    long headerSize = stream.getPos() - pageStart;
    repetitionLevels.writeAllTo(stream);
    definitionLevels.writeAllTo(stream);
    dataBytes.writeAllTo(stream);

    recordDataPage(pageStart, headerSize, uncompressedSize, compressedSize, rowCount, statistics);
    columnEncodings.add(dataEncoding);
    columnEncodingStats.withV2Pages();
    columnEncodingStats.addDataEncoding(dataEncoding);
  }

  /** Supplies the size and geospatial statistics of the next data page written. */
  void pageStatistics(SizeStatistics sizeStatistics, GeospatialStatistics geospatialStatistics) {
    Preconditions.checkState(state == State.COLUMN, "Cannot add statistics in state %s", state);
    this.nextPageSizeStatistics = sizeStatistics;
    if (geospatialStatistics == null) {
      return;
    }

    if (columnGeospatialStatistics == null) {
      this.columnGeospatialStatistics = geospatialStatistics.copy();
    } else {
      columnGeospatialStatistics.merge(geospatialStatistics);
    }
  }

  /** Registers a bloom filter for the column with the given dot-separated path. */
  void addBloomFilter(String columnPath, BloomFilter bloomFilter) {
    bloomFilters.put(columnPath, bloomFilter);
  }

  void endColumn() throws IOException {
    Preconditions.checkState(state == State.COLUMN, "Cannot end a column in state %s", state);
    PrimitiveType type = columnDescriptor.getPrimitiveType();

    ColumnMetaData columnMeta = new ColumnMetaData();
    columnMeta.setType(toParquetType(type.getPrimitiveTypeName()));
    columnMeta.setEncodings(toFormatEncodings(columnEncodings));
    columnMeta.setPath_in_schema(Lists.newArrayList(columnDescriptor.getPath()));
    columnMeta.setCodec(columnCodec.getParquetCompressionCodec());
    columnMeta.setNum_values(columnValueCount);
    columnMeta.setTotal_uncompressed_size(columnUncompressedSize);
    columnMeta.setTotal_compressed_size(columnCompressedSize);
    columnMeta.setData_page_offset(columnFirstDataPageOffset);
    if (columnDictionaryOffset >= 0) {
      columnMeta.setDictionary_page_offset(columnDictionaryOffset);
    }

    if (columnStatisticsValid && columnStatistics != null && !columnStatistics.isEmpty()) {
      columnMeta.setStatistics(ParquetMetadataConverter.toParquetStatistics(columnStatistics));
    }

    if (columnSizeStatistics.isValid()) {
      columnMeta.setSize_statistics(
          ParquetMetadataConverter.toParquetSizeStatistics(columnSizeStatistics));
    }

    if (columnGeospatialStatistics != null && columnGeospatialStatistics.isValid()) {
      columnMeta.setGeospatial_statistics(toFormatGeospatial(columnGeospatialStatistics));
    }

    List<org.apache.parquet.format.PageEncodingStats> encodingStats =
        CONVERTER.convertEncodingStats(columnEncodingStats.build());
    if (encodingStats != null && !encodingStats.isEmpty()) {
      columnMeta.setEncoding_stats(encodingStats);
    }

    ColumnChunk chunk = new ColumnChunk();
    chunk.setFile_offset(columnStart);
    chunk.setMeta_data(columnMeta);

    org.apache.parquet.internal.column.columnindex.ColumnIndex columnIndex =
        columnIndexBuilder.build();
    pendingIndexes.add(
        new PendingIndexes(
            chunk,
            columnIndex != null
                ? ParquetMetadataConverter.toParquetColumnIndex(type, columnIndex)
                : null,
            ParquetMetadataConverter.toParquetOffsetIndex(
                offsetIndexBuilder.build(columnFirstDataPageOffset)),
            bloomFilters.remove(ColumnPath.get(columnDescriptor.getPath()).toDotString()),
            rowGroups.size(),
            columnOrdinal));

    blockColumns.add(chunk);
    blockUncompressedSize += columnUncompressedSize;
    blockCompressedSize += columnCompressedSize;
    state = State.BLOCK;
  }

  void endBlock() {
    endBlock(null);
  }

  void endBlock(List<SortingColumn> sortingColumns) {
    Preconditions.checkState(state == State.BLOCK, "Cannot end a block in state %s", state);
    Preconditions.checkState(
        rowGroups.size() < Short.MAX_VALUE, "Too many row groups: %s", rowGroups.size());

    RowGroup rowGroup = new RowGroup(blockColumns, blockUncompressedSize, blockRowCount);
    rowGroup.setFile_offset(blockStart);
    rowGroup.setTotal_compressed_size(blockCompressedSize);
    rowGroup.setOrdinal((short) rowGroups.size());
    if (sortingColumns != null && !sortingColumns.isEmpty()) {
      rowGroup.setSorting_columns(sortingColumns);
    }

    rowGroups.add(rowGroup);
    state = State.STARTED;
  }

  void appendFile(org.apache.parquet.io.InputFile inputFile) throws IOException {
    appendFile(new ParquetInputFileWrapper(inputFile));
  }

  /** Appends all row groups of a file with the same schema without decoding the pages. */
  void appendFile(org.apache.iceberg.io.InputFile inputFile) throws IOException {
    Preconditions.checkState(state == State.STARTED, "Cannot append a file in state %s", state);
    Preconditions.checkState(
        fileEncryptor == null, "Cannot append files to an encrypted Parquet file");

    try (NativeParquetFileReader reader = NativeParquetFileReader.open(inputFile)) {
      Preconditions.checkArgument(
          schema.getFields().equals(reader.fileSchema().getFields()),
          "Cannot append %s: schema does not match the writer schema",
          inputFile.location());

      for (BlockMetadata block : reader.rowGroups()) {
        padToRowGroupBoundary();
        long blockOffset = stream.getPos();
        List<ColumnChunk> columns = Lists.newArrayList();
        long uncompressed = 0;
        long compressed = 0;

        for (ColumnChunkMetadata column : block.columns()) {
          ColumnChunk chunk = copyColumnChunk(reader, column);
          columns.add(chunk);
          uncompressed += column.totalUncompressedSize();
          compressed += column.totalSize();
        }

        RowGroup rowGroup = new RowGroup(columns, uncompressed, block.rowCount());
        rowGroup.setFile_offset(blockOffset);
        rowGroup.setTotal_compressed_size(compressed);
        rowGroup.setOrdinal((short) rowGroups.size());
        if (block.sortingColumns() != null && !block.sortingColumns().isEmpty()) {
          rowGroup.setSorting_columns(block.sortingColumns());
        }

        rowGroups.add(rowGroup);
      }
    }
  }

  private ColumnChunk copyColumnChunk(NativeParquetFileReader reader, ColumnChunkMetadata column)
      throws IOException {
    long sourceStart = column.getStartingPos();
    long targetStart = stream.getPos();
    long delta = targetStart - sourceStart;

    org.apache.parquet.format.ColumnIndex columnIndex = reader.readColumnIndex(column);
    org.apache.parquet.format.OffsetIndex offsetIndex = reader.readOffsetIndex(column);
    BloomFilter bloomFilter = reader.readBloomFilter(column);
    reader.copyRange(sourceStart, column.totalSize(), stream);

    ColumnMetaData columnMeta = new ColumnMetaData();
    columnMeta.setType(toParquetType(column.typeName()));
    columnMeta.setEncodings(toFormatEncodings(column.encodings()));
    columnMeta.setPath_in_schema(Lists.newArrayList(column.path().toArray()));
    columnMeta.setCodec(column.codec().getParquetCompressionCodec());
    columnMeta.setNum_values(column.valueCount());
    columnMeta.setTotal_uncompressed_size(column.totalUncompressedSize());
    columnMeta.setTotal_compressed_size(column.totalSize());
    columnMeta.setData_page_offset(column.firstDataPageOffset() + delta);
    if (column.dictionaryPageOffset() > 0) {
      columnMeta.setDictionary_page_offset(column.dictionaryPageOffset() + delta);
    }

    if (column.statistics() != null && !column.statistics().isEmpty()) {
      columnMeta.setStatistics(ParquetMetadataConverter.toParquetStatistics(column.statistics()));
    }

    if (column.encodingStats() != null) {
      columnMeta.setEncoding_stats(CONVERTER.convertEncodingStats(column.encodingStats()));
    }

    ColumnChunkExtras extras = column.extras();
    if (extras.sizeStatistics() != null) {
      columnMeta.setSize_statistics(extras.sizeStatistics());
    }

    if (extras.geospatialStatistics() != null) {
      columnMeta.setGeospatial_statistics(extras.geospatialStatistics());
    }

    if (extras.keyValueMetadata() != null) {
      columnMeta.setKey_value_metadata(extras.keyValueMetadata());
    }

    if (offsetIndex != null) {
      offsetIndex
          .getPage_locations()
          .forEach(location -> location.setOffset(location.getOffset() + delta));
    }

    ColumnChunk chunk = new ColumnChunk();
    chunk.setFile_offset(targetStart);
    chunk.setMeta_data(columnMeta);
    pendingIndexes.add(
        new PendingIndexes(
            chunk,
            columnIndex,
            offsetIndex,
            bloomFilter,
            rowGroups.size(),
            columnOrdinalOf(schema.getColumnDescription(column.path().toArray()))));
    return chunk;
  }

  void end(Map<String, String> extraMetadata) throws IOException {
    Preconditions.checkState(state == State.STARTED, "Cannot end the file in state %s", state);

    try {
      writeIndexes();

      FileMetaData fileMetadata = new FileMetaData();
      fileMetadata.setVersion(1);
      fileMetadata.setSchema(toParquetSchema(schema));
      fileMetadata.setNum_rows(rowGroups.stream().mapToLong(RowGroup::getNum_rows).sum());
      fileMetadata.setRow_groups(rowGroups);
      fileMetadata.setCreated_by(Version.FULL_VERSION);
      fileMetadata.setColumn_orders(columnOrders(schema.getColumns().size()));
      this.footer = NativeParquetFileReader.parseMetadata(fileMetadata);
      protectColumnMetadata();

      if (extraMetadata != null && !extraMetadata.isEmpty()) {
        List<KeyValue> keyValues = Lists.newArrayList();
        extraMetadata.forEach((key, value) -> keyValues.add(new KeyValue(key).setValue(value)));
        fileMetadata.setKey_value_metadata(keyValues);
      }

      writeFooter(fileMetadata);
      state = State.ENDED;
    } catch (IOException | RuntimeException e) {
      aborted = true;
      throw e;
    }
  }

  /** Closes the output without writing a footer, leaving an incomplete file. */
  void abort() throws IOException {
    aborted = true;
    stream.close();
  }

  @Override
  public void close() throws IOException {
    try {
      if (state == State.STARTED && !aborted) {
        end(null);
      }
    } finally {
      stream.close();
    }
  }

  private void protectColumnMetadata() {
    if (fileEncryptor == null) {
      return;
    }

    for (PendingIndexes pending : pendingIndexes) {
      ColumnChunk chunk = pending.chunk;
      ColumnMetaData columnMeta = chunk.getMeta_data();
      InternalColumnEncryptionSetup setup =
          fileEncryptor.getColumnSetup(
              ColumnPath.get(columnMeta.getPath_in_schema().toArray(new String[0])),
              true,
              pending.columnOrdinal);
      if (!setup.isEncrypted()) {
        continue;
      }

      chunk.setCrypto_metadata(setup.getColumnCryptoMetaData());
      if (!fileEncryptor.encryptColumnMetaData(setup)) {
        continue;
      }

      ByteArrayOutputStream encrypted = new ByteArrayOutputStream();
      try {
        Util.writeColumnMetaData(
            columnMeta,
            encrypted,
            setup.getMetaDataEncryptor(),
            AesCipher.createModuleAAD(
                fileEncryptor.getFileAAD(),
                ModuleType.ColumnMetaData,
                pending.rowGroupOrdinal,
                pending.columnOrdinal,
                -1));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to encrypt column metadata", e);
      }

      chunk.setEncrypted_column_metadata(encrypted.toByteArray());
      if (fileEncryptor.isFooterEncrypted()) {
        chunk.unsetMeta_data();
      } else {
        ColumnMetaData redacted = columnMeta.deepCopy();
        redacted.unsetStatistics();
        redacted.unsetEncoding_stats();
        chunk.setMeta_data(redacted);
      }
    }
  }

  private void writeFooter(FileMetaData fileMetadata) throws IOException {
    long footerStart = stream.getPos();
    if (fileEncryptor == null) {
      Util.writeFileMetaData(fileMetadata, stream);
      writeIntLittleEndian(stream, Math.toIntExact(stream.getPos() - footerStart));
      stream.write(MAGIC);
    } else if (fileEncryptor.isFooterEncrypted()) {
      Util.writeFileCryptoMetaData(fileEncryptor.getFileCryptoMetaData(), stream);
      Util.writeFileMetaData(
          fileMetadata,
          stream,
          fileEncryptor.getFooterEncryptor(),
          AesCipher.createFooterAAD(fileEncryptor.getFileAAD()));
      writeIntLittleEndian(stream, Math.toIntExact(stream.getPos() - footerStart));
      stream.write(ENCRYPTED_MAGIC);
    } else {
      fileMetadata.setEncryption_algorithm(fileEncryptor.getEncryptionAlgorithm());
      byte[] signingKeyMetadata = fileEncryptor.getFooterSigningKeyMetaData();
      if (signingKeyMetadata != null) {
        fileMetadata.setFooter_signing_key_metadata(signingKeyMetadata);
      }

      ByteArrayOutputStream serialized = new ByteArrayOutputStream();
      Util.writeFileMetaData(fileMetadata, serialized);
      byte[] footerBytes = serialized.toByteArray();
      byte[] signed =
          fileEncryptor
              .getSignedFooterEncryptor()
              .encrypt(footerBytes, AesCipher.createFooterAAD(fileEncryptor.getFileAAD()));
      stream.write(footerBytes);
      stream.write(signed, ModuleCipherFactory.SIZE_LENGTH, AesCipher.NONCE_LENGTH);
      stream.write(signed, signed.length - AesCipher.GCM_TAG_LENGTH, AesCipher.GCM_TAG_LENGTH);
      writeIntLittleEndian(stream, Math.toIntExact(stream.getPos() - footerStart));
      stream.write(MAGIC);
    }
  }

  private void writeIndexes() throws IOException {
    for (PendingIndexes pending : pendingIndexes) {
      if (pending.columnIndex != null) {
        long start = stream.getPos();
        BlockCipher.Encryptor encryptor = metadataEncryptor(pending);
        if (encryptor == null) {
          Util.writeColumnIndex(pending.columnIndex, stream);
        } else {
          Util.writeColumnIndex(
              pending.columnIndex, stream, encryptor, pendingAad(pending, ModuleType.ColumnIndex));
        }

        pending.chunk.setColumn_index_offset(start);
        pending.chunk.setColumn_index_length(Math.toIntExact(stream.getPos() - start));
      }
    }

    for (PendingIndexes pending : pendingIndexes) {
      if (pending.offsetIndex != null) {
        long start = stream.getPos();
        BlockCipher.Encryptor encryptor = metadataEncryptor(pending);
        if (encryptor == null) {
          Util.writeOffsetIndex(pending.offsetIndex, stream);
        } else {
          Util.writeOffsetIndex(
              pending.offsetIndex, stream, encryptor, pendingAad(pending, ModuleType.OffsetIndex));
        }

        pending.chunk.setOffset_index_offset(start);
        pending.chunk.setOffset_index_length(Math.toIntExact(stream.getPos() - start));
      }
    }

    for (PendingIndexes pending : pendingIndexes) {
      if (pending.bloomFilter != null) {
        long start = stream.getPos();
        BloomFilterHeader header =
            ParquetMetadataConverter.toBloomFilterHeader(pending.bloomFilter);
        BlockCipher.Encryptor encryptor = metadataEncryptor(pending);
        if (encryptor == null) {
          Util.writeBloomFilterHeader(header, stream);
          pending.bloomFilter.writeTo(stream);
        } else {
          Util.writeBloomFilterHeader(
              header, stream, encryptor, pendingAad(pending, ModuleType.BloomFilterHeader));
          ByteArrayOutputStream bitset = new ByteArrayOutputStream();
          pending.bloomFilter.writeTo(bitset);
          stream.write(
              encryptor.encrypt(
                  bitset.toByteArray(), pendingAad(pending, ModuleType.BloomFilterBitset)));
        }
        pending.chunk.getMeta_data().setBloom_filter_offset(start);
        pending
            .chunk
            .getMeta_data()
            .setBloom_filter_length(Math.toIntExact(stream.getPos() - start));
      }
    }
  }

  private void recordDataPage(
      long pageStart,
      long headerSize,
      long uncompressedPageSize,
      long compressedPageSize,
      long rowCount,
      Statistics<?> statistics) {
    if (columnFirstDataPageOffset < 0) {
      columnFirstDataPageOffset = pageStart;
    }

    columnUncompressedSize += headerSize + uncompressedPageSize;
    columnCompressedSize += headerSize + compressedPageSize;
    offsetIndexBuilder.add(Math.toIntExact(headerSize + compressedPageSize), rowCount);

    SizeStatistics pageSize = nextPageSizeStatistics;
    this.nextPageSizeStatistics = null;
    columnSizeStatistics.mergeStatistics(pageSize);
    if (!columnSizeStatistics.isValid()) {
      pageSize = null;
    }

    if (statistics == null) {
      columnStatisticsValid = false;
      columnIndexBuilder.add(Statistics.createStats(columnDescriptor.getPrimitiveType()));
    } else {
      if (columnStatistics == null) {
        this.columnStatistics = statistics.copy();
      } else {
        columnStatistics.mergeStatistics(statistics);
      }

      columnIndexBuilder.add(statistics, pageSize);
    }
  }

  private BlockCipher.Encryptor metadataEncryptor(PendingIndexes pending) {
    if (fileEncryptor == null) {
      return null;
    }

    InternalColumnEncryptionSetup setup =
        fileEncryptor.getColumnSetup(
            ColumnPath.get(pending.chunk.getMeta_data().getPath_in_schema().toArray(new String[0])),
            true,
            pending.columnOrdinal);
    return setup.isEncrypted() ? setup.getMetaDataEncryptor() : null;
  }

  private byte[] pendingAad(PendingIndexes pending, ModuleType type) {
    return AesCipher.createModuleAAD(
        fileEncryptor.getFileAAD(), type, pending.rowGroupOrdinal, pending.columnOrdinal, -1);
  }

  private void writeHeader(PageHeader header, ModuleType type, int page) throws IOException {
    if (columnHeaderEncryptor == null) {
      Util.writePageHeader(header, stream);
    } else {
      Util.writePageHeader(header, stream, columnHeaderEncryptor, moduleAad(type, page));
    }
  }

  private void setCrc(PageHeader header, BytesInput bytes) throws IOException {
    if (pageChecksums) {
      CRC32 crc = new CRC32();
      crc.update(bytes.toByteArray());
      header.setCrc((int) crc.getValue());
    }
  }

  private void padToRowGroupBoundary() throws IOException {
    if (rowGroupSize <= 0 || maxPaddingSize <= 0) {
      return;
    }

    long remaining = rowGroupSize - (stream.getPos() % rowGroupSize);
    if (remaining > 0 && remaining < rowGroupSize && remaining <= maxPaddingSize) {
      stream.write(new byte[(int) remaining]);
    }
  }

  private static org.apache.parquet.format.GeospatialStatistics toFormatGeospatial(
      GeospatialStatistics statistics) {
    org.apache.parquet.format.GeospatialStatistics result =
        new org.apache.parquet.format.GeospatialStatistics();
    BoundingBox box = statistics.getBoundingBox();
    if (box != null && box.isValid() && !box.isXYEmpty()) {
      org.apache.parquet.format.BoundingBox bbox =
          new org.apache.parquet.format.BoundingBox(
              box.getXMin(), box.getXMax(), box.getYMin(), box.getYMax());
      if (box.isZValid() && !box.isZEmpty()) {
        bbox.setZmin(box.getZMin());
        bbox.setZmax(box.getZMax());
      }

      if (box.isMValid() && !box.isMEmpty()) {
        bbox.setMmin(box.getMMin());
        bbox.setMmax(box.getMMax());
      }

      result.setBbox(bbox);
    }

    GeospatialTypes types = statistics.getGeospatialTypes();
    if (types != null && types.isValid()) {
      result.setGeospatial_types(Lists.newArrayList(types.getTypes()));
    }

    return result;
  }

  private static List<ColumnOrder> columnOrders(int columnCount) {
    List<ColumnOrder> orders = Lists.newArrayListWithCapacity(columnCount);
    for (int i = 0; i < columnCount; i++) {
      orders.add(ColumnOrder.TYPE_ORDER(new TypeDefinedOrder()));
    }

    return orders;
  }

  private static List<org.apache.parquet.format.Encoding> toFormatEncodings(
      Set<Encoding> encodings) {
    List<org.apache.parquet.format.Encoding> result = Lists.newArrayList();
    for (Encoding encoding : encodings) {
      result.add(CONVERTER.getEncoding(encoding));
    }

    return result;
  }

  private static List<SchemaElement> toParquetSchema(MessageType schema) {
    List<SchemaElement> result = Lists.newArrayList();
    SchemaElement root = new SchemaElement(schema.getName());
    root.setNum_children(schema.getFieldCount());
    result.add(root);
    for (Type field : schema.getFields()) {
      addSchemaElement(result, field);
    }

    return result;
  }

  private static void addSchemaElement(List<SchemaElement> result, Type type) {
    SchemaElement element = new SchemaElement(type.getName());
    element.setRepetition_type(FieldRepetitionType.valueOf(type.getRepetition().name()));

    if (type.getId() != null) {
      element.setField_id(type.getId().intValue());
    }

    LogicalTypeAnnotation annotation = type.getLogicalTypeAnnotation();
    if (annotation != null) {
      if (annotation.toOriginalType() != null) {
        element.setConverted_type(ConvertedType.valueOf(annotation.toOriginalType().name()));
      } else if (annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation) {
        element.setConverted_type(ConvertedType.MAP_KEY_VALUE);
      }

      LogicalType logicalType = annotation.accept(LOGICAL_TYPES).orElse(null);
      if (logicalType != null) {
        element.setLogicalType(logicalType);
      }

      if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
        element.setPrecision(decimal.getPrecision());
        element.setScale(decimal.getScale());
      }
    }

    if (type.isPrimitive()) {
      PrimitiveType primitive = type.asPrimitiveType();
      element.setType(toParquetType(primitive.getPrimitiveTypeName()));
      if (primitive.getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        element.setType_length(primitive.getTypeLength());
      }

      result.add(element);
    } else {
      GroupType group = type.asGroupType();
      element.setNum_children(group.getFieldCount());
      result.add(element);
      for (Type field : group.getFields()) {
        addSchemaElement(result, field);
      }
    }
  }

  private static org.apache.parquet.format.Type toParquetType(PrimitiveTypeName typeName) {
    return switch (typeName) {
      case BOOLEAN -> org.apache.parquet.format.Type.BOOLEAN;
      case INT32 -> org.apache.parquet.format.Type.INT32;
      case INT64 -> org.apache.parquet.format.Type.INT64;
      case INT96 -> org.apache.parquet.format.Type.INT96;
      case FLOAT -> org.apache.parquet.format.Type.FLOAT;
      case DOUBLE -> org.apache.parquet.format.Type.DOUBLE;
      case BINARY -> org.apache.parquet.format.Type.BYTE_ARRAY;
      case FIXED_LEN_BYTE_ARRAY -> org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY;
    };
  }

  private static void writeIntLittleEndian(PositionOutputStream out, int value) throws IOException {
    out.write(value & 0xFF);
    out.write((value >> 8) & 0xFF);
    out.write((value >> 16) & 0xFF);
    out.write((value >> 24) & 0xFF);
  }

  private static final class PendingIndexes {
    private final ColumnChunk chunk;
    private final org.apache.parquet.format.ColumnIndex columnIndex;
    private final org.apache.parquet.format.OffsetIndex offsetIndex;
    private final BloomFilter bloomFilter;
    private final int rowGroupOrdinal;
    private final int columnOrdinal;

    PendingIndexes(
        ColumnChunk chunk,
        org.apache.parquet.format.ColumnIndex columnIndex,
        org.apache.parquet.format.OffsetIndex offsetIndex,
        BloomFilter bloomFilter,
        int rowGroupOrdinal,
        int columnOrdinal) {
      this.rowGroupOrdinal = rowGroupOrdinal;
      this.columnOrdinal = columnOrdinal;
      this.chunk = chunk;
      this.columnIndex = columnIndex;
      this.offsetIndex = offsetIndex;
      this.bloomFilter = bloomFilter;
    }
  }

  private static final class LogicalTypeVisitor
      implements LogicalTypeAnnotationVisitor<LogicalType> {
    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.STRING(new org.apache.parquet.format.StringType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.MAP(new org.apache.parquet.format.MapType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.LIST(new org.apache.parquet.format.ListType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.ENUM(new org.apache.parquet.format.EnumType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation type) {
      return Optional.of(
          LogicalType.DECIMAL(
              new org.apache.parquet.format.DecimalType(type.getScale(), type.getPrecision())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.DATE(new org.apache.parquet.format.DateType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation type) {
      return Optional.of(
          LogicalType.TIME(
              new org.apache.parquet.format.TimeType(
                  type.isAdjustedToUTC(), toFormatUnit(type.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation type) {
      return Optional.of(
          LogicalType.TIMESTAMP(
              new org.apache.parquet.format.TimestampType(
                  type.isAdjustedToUTC(), toFormatUnit(type.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation type) {
      return Optional.of(
          LogicalType.INTEGER(
              new org.apache.parquet.format.IntType((byte) type.getBitWidth(), type.isSigned())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.JSON(new org.apache.parquet.format.JsonType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.BSON(new org.apache.parquet.format.BsonType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.UUID(new org.apache.parquet.format.UUIDType()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.Float16LogicalTypeAnnotation type) {
      return Optional.of(LogicalType.FLOAT16(new org.apache.parquet.format.Float16Type()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.VariantLogicalTypeAnnotation type) {
      org.apache.parquet.format.VariantType variant = new org.apache.parquet.format.VariantType();
      variant.setSpecification_version(type.getSpecVersion());
      return Optional.of(LogicalType.VARIANT(variant));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation type) {
      GeometryType geometry = new GeometryType();
      if (type.getCrs() != null) {
        geometry.setCrs(type.getCrs());
      }

      return Optional.of(LogicalType.GEOMETRY(geometry));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.GeographyLogicalTypeAnnotation type) {
      GeographyType geography = new GeographyType();
      if (type.getCrs() != null) {
        geography.setCrs(type.getCrs());
      }

      if (type.getAlgorithm() != null) {
        geography.setAlgorithm(
            org.apache.parquet.format.EdgeInterpolationAlgorithm.valueOf(
                type.getAlgorithm().name()));
      }

      return Optional.of(LogicalType.GEOGRAPHY(geography));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.UnknownLogicalTypeAnnotation type) {
      return Optional.of(LogicalType.UNKNOWN(new org.apache.parquet.format.NullType()));
    }

    private static org.apache.parquet.format.TimeUnit toFormatUnit(
        LogicalTypeAnnotation.TimeUnit unit) {
      return switch (unit) {
        case MILLIS ->
            org.apache.parquet.format.TimeUnit.MILLIS(new org.apache.parquet.format.MilliSeconds());
        case MICROS ->
            org.apache.parquet.format.TimeUnit.MICROS(new org.apache.parquet.format.MicroSeconds());
        case NANOS ->
            org.apache.parquet.format.TimeUnit.NANOS(new org.apache.parquet.format.NanoSeconds());
      };
    }
  }

  /** Wrapper to adapt parquet InputFile to iceberg InputFile */
  private static class ParquetInputFileWrapper implements org.apache.iceberg.io.InputFile {
    private final org.apache.parquet.io.InputFile delegate;

    ParquetInputFileWrapper(org.apache.parquet.io.InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      try {
        return delegate.getLength();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public org.apache.iceberg.io.SeekableInputStream newStream() {
      try {
        return new SeekableInputStreamWrapper(delegate.newStream());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public String location() {
      return delegate.toString();
    }

    @Override
    public boolean exists() {
      return true;
    }
  }

  /** Wrapper to adapt parquet SeekableInputStream to iceberg SeekableInputStream */
  private static class SeekableInputStreamWrapper
      extends org.apache.iceberg.io.SeekableInputStream {
    private final org.apache.parquet.io.SeekableInputStream delegate;

    SeekableInputStreamWrapper(org.apache.parquet.io.SeekableInputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
