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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
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

  private final PositionOutputStream stream;
  private final MessageType schema;
  private final List<RowGroup> rowGroups;
  private final Map<String, String> extraMetadata;

  private boolean closed;

  private NativeParquetFileWriter(PositionOutputStream stream, MessageType schema) {
    this.stream = stream;
    this.schema = schema;
    this.rowGroups = Lists.newArrayList();
    this.extraMetadata = Maps.newHashMap();
    this.closed = false;
  }

  static NativeParquetFileWriter create(
      OutputFile file, MessageType schema, CompressionCodecName codec) throws IOException {
    PositionOutputStream stream = file.create();

    try {
      stream.write(MAGIC);
      return new NativeParquetFileWriter(stream, schema);
    } catch (IOException e) {
      stream.close();
      throw e;
    }
  }

  void appendFile(org.apache.parquet.io.InputFile inputFile) throws IOException {
    // Wrap parquet InputFile as iceberg InputFile
    org.apache.iceberg.io.InputFile icebergInputFile = new ParquetInputFileWrapper(inputFile);

    // Read source file metadata
    try (NativeParquetFileReader reader = NativeParquetFileReader.open(icebergInputFile)) {
      org.apache.iceberg.parquet.metadata.ParquetMetadata metadata = reader.metadata();

      // Copy each row group
      for (org.apache.iceberg.parquet.metadata.BlockMetadata block : metadata.blocks()) {
        RowGroup rowGroup = copyRowGroup(inputFile, block);
        rowGroups.add(rowGroup);
      }
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
        org.apache.parquet.io.SeekableInputStream parquetStream = delegate.newStream();
        return new SeekableInputStreamWrapper(parquetStream);
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

  private RowGroup copyRowGroup(
      org.apache.parquet.io.InputFile inputFile,
      org.apache.iceberg.parquet.metadata.BlockMetadata block)
      throws IOException {
    RowGroup rowGroup = new RowGroup();
    rowGroup.setNum_rows(block.rowCount());

    List<ColumnChunk> columns = Lists.newArrayList();

    try (org.apache.parquet.io.SeekableInputStream input = inputFile.newStream()) {
      for (org.apache.iceberg.parquet.metadata.ColumnChunkMetadata columnMeta : block.columns()) {
        ColumnChunk columnChunk = copyColumnChunk(input, columnMeta);
        columns.add(columnChunk);
      }
    }

    rowGroup.setColumns(columns);
    return rowGroup;
  }

  private ColumnChunk copyColumnChunk(
      org.apache.parquet.io.SeekableInputStream input,
      org.apache.iceberg.parquet.metadata.ColumnChunkMetadata columnMeta)
      throws IOException {
    long sourceOffset = columnMeta.getStartingPos();
    long chunkSize = columnMeta.totalSize();

    // Record where we'll write this chunk in output file
    long targetOffset = stream.getPos();

    // Copy chunk data
    input.seek(sourceOffset);
    byte[] buffer = new byte[8192];
    long remaining = chunkSize;
    while (remaining > 0) {
      int toRead = (int) Math.min(buffer.length, remaining);
      int bytesRead = input.read(buffer, 0, toRead);
      if (bytesRead < 0) {
        throw new IOException("Unexpected end of input file");
      }
      stream.write(buffer, 0, bytesRead);
      remaining -= bytesRead;
    }

    // Build column metadata
    ColumnMetaData columnMetadata = new ColumnMetaData();
    columnMetadata.setType(toParquetType(columnMeta.primitiveType().getPrimitiveTypeName()));
    columnMetadata.setEncodings(
        columnMeta.encodings().stream()
            .map(NativeParquetFileWriter::toFormatEncoding)
            .collect(java.util.stream.Collectors.toList()));
    columnMetadata.setPath_in_schema(
        java.util.Arrays.stream(columnMeta.path().toArray())
            .collect(java.util.stream.Collectors.toList()));
    columnMetadata.setCodec(toFormatCodec(columnMeta.codec()));
    columnMetadata.setNum_values(columnMeta.valueCount());
    columnMetadata.setTotal_uncompressed_size(columnMeta.totalUncompressedSize());
    columnMetadata.setTotal_compressed_size(columnMeta.totalSize());
    columnMetadata.setData_page_offset(targetOffset);

    if (columnMeta.dictionaryPageOffset() > 0) {
      // Dictionary page offset relative to start of column chunk
      long dictOffsetFromChunkStart = columnMeta.dictionaryPageOffset() - sourceOffset;
      columnMetadata.setDictionary_page_offset(targetOffset + dictOffsetFromChunkStart);
    }

    // Copy statistics if present
    if (columnMeta.statistics() != null && !columnMeta.statistics().isEmpty()) {
      columnMetadata.setStatistics(
          toFormatStatistics(
              org.apache.parquet.column.statistics.Statistics.getStatsBasedOnType(
                  columnMeta.primitiveType().getPrimitiveTypeName())));
    }

    ColumnChunk columnChunk = new ColumnChunk();
    columnChunk.setFile_offset(targetOffset);
    columnChunk.setMeta_data(columnMetadata);

    return columnChunk;
  }

  private static org.apache.parquet.format.CompressionCodec toFormatCodec(
      CompressionCodecName codec) {
    switch (codec) {
      case UNCOMPRESSED:
        return org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
      case SNAPPY:
        return org.apache.parquet.format.CompressionCodec.SNAPPY;
      case GZIP:
        return org.apache.parquet.format.CompressionCodec.GZIP;
      case LZO:
        return org.apache.parquet.format.CompressionCodec.LZO;
      case BROTLI:
        return org.apache.parquet.format.CompressionCodec.BROTLI;
      case LZ4:
        return org.apache.parquet.format.CompressionCodec.LZ4;
      case ZSTD:
        return org.apache.parquet.format.CompressionCodec.ZSTD;
      case LZ4_RAW:
        return org.apache.parquet.format.CompressionCodec.LZ4_RAW;
      default:
        return org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
    }
  }

  void end(Map<String, String> metadata) throws IOException {
    if (closed) {
      throw new IllegalStateException("Writer already closed");
    }

    // Merge provided metadata with accumulated metadata
    Map<String, String> allMetadata = Maps.newHashMap(extraMetadata);
    if (metadata != null) {
      allMetadata.putAll(metadata);
    }

    // Build file metadata
    FileMetaData fileMetadata = new FileMetaData();
    fileMetadata.setVersion(1);
    fileMetadata.setSchema(toParquetSchema(schema));
    fileMetadata.setNum_rows(calculateTotalRowCount());
    fileMetadata.setRow_groups(rowGroups);
    fileMetadata.setCreated_by("parquet-mr version 1.13.1 (build unknown)");

    if (!allMetadata.isEmpty()) {
      List<org.apache.parquet.format.KeyValue> keyValues = Lists.newArrayList();
      for (Map.Entry<String, String> entry : allMetadata.entrySet()) {
        keyValues.add(
            new org.apache.parquet.format.KeyValue(entry.getKey()).setValue(entry.getValue()));
      }
      fileMetadata.setKey_value_metadata(keyValues);
    }

    // Write footer
    long footerStart = stream.getPos();
    Util.writeFileMetaData(fileMetadata, stream);
    long footerLength = stream.getPos() - footerStart;

    // Write footer length (4 bytes little-endian)
    writeIntLittleEndian(stream, (int) footerLength);

    // Write magic bytes at end
    stream.write(MAGIC);

    closed = true;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      end(null);
    }
    stream.close();
  }

  private long calculateTotalRowCount() {
    long total = 0;
    for (RowGroup rowGroup : rowGroups) {
      total += rowGroup.getNum_rows();
    }
    return total;
  }

  private static List<SchemaElement> toParquetSchema(MessageType schema) {
    List<SchemaElement> result = Lists.newArrayList();
    addSchemaElement(result, schema);
    return result;
  }

  private static void addSchemaElement(List<SchemaElement> result, Type type) {
    SchemaElement element = new SchemaElement(type.getName());

    // Set field ID if present
    if (type.getId() != null) {
      element.setField_id(type.getId().intValue());
    }

    if (type.isPrimitive()) {
      PrimitiveType primitiveType = type.asPrimitiveType();
      element.setType(toParquetType(primitiveType.getPrimitiveTypeName()));
      element.setRepetition_type(toParquetRepetition(type.getRepetition()));

      if (primitiveType.getOriginalType() != null) {
        element.setConverted_type(toParquetConvertedType(primitiveType.getOriginalType()));
      }

      if (primitiveType.getDecimalMetadata() != null) {
        element.setPrecision(primitiveType.getDecimalMetadata().getPrecision());
        element.setScale(primitiveType.getDecimalMetadata().getScale());
      }

      if (primitiveType.getTypeLength() > 0) {
        element.setType_length(primitiveType.getTypeLength());
      }
    } else {
      GroupType groupType = type.asGroupType();
      element.setRepetition_type(toParquetRepetition(type.getRepetition()));
      element.setNum_children(groupType.getFieldCount());

      if (groupType.getOriginalType() != null) {
        element.setConverted_type(toParquetConvertedType(groupType.getOriginalType()));
      }
    }

    result.add(element);

    if (!type.isPrimitive()) {
      GroupType groupType = type.asGroupType();
      for (Type field : groupType.getFields()) {
        addSchemaElement(result, field);
      }
    }
  }

  private static org.apache.parquet.format.Type toParquetType(PrimitiveTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return org.apache.parquet.format.Type.BOOLEAN;
      case INT32:
        return org.apache.parquet.format.Type.INT32;
      case INT64:
        return org.apache.parquet.format.Type.INT64;
      case INT96:
        return org.apache.parquet.format.Type.INT96;
      case FLOAT:
        return org.apache.parquet.format.Type.FLOAT;
      case DOUBLE:
        return org.apache.parquet.format.Type.DOUBLE;
      case BINARY:
        return org.apache.parquet.format.Type.BYTE_ARRAY;
      case FIXED_LEN_BYTE_ARRAY:
        return org.apache.parquet.format.Type.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new IllegalArgumentException("Unknown type: " + typeName);
    }
  }

  private static FieldRepetitionType toParquetRepetition(Type.Repetition repetition) {
    switch (repetition) {
      case REQUIRED:
        return FieldRepetitionType.REQUIRED;
      case OPTIONAL:
        return FieldRepetitionType.OPTIONAL;
      case REPEATED:
        return FieldRepetitionType.REPEATED;
      default:
        throw new IllegalArgumentException("Unknown repetition: " + repetition);
    }
  }

  private static ConvertedType toParquetConvertedType(
      org.apache.parquet.schema.OriginalType originalType) {
    switch (originalType) {
      case UTF8:
        return ConvertedType.UTF8;
      case MAP:
        return ConvertedType.MAP;
      case MAP_KEY_VALUE:
        return ConvertedType.MAP_KEY_VALUE;
      case LIST:
        return ConvertedType.LIST;
      case ENUM:
        return ConvertedType.ENUM;
      case DECIMAL:
        return ConvertedType.DECIMAL;
      case DATE:
        return ConvertedType.DATE;
      case TIME_MILLIS:
        return ConvertedType.TIME_MILLIS;
      case TIME_MICROS:
        return ConvertedType.TIME_MICROS;
      case TIMESTAMP_MILLIS:
        return ConvertedType.TIMESTAMP_MILLIS;
      case TIMESTAMP_MICROS:
        return ConvertedType.TIMESTAMP_MICROS;
      case INTERVAL:
        return ConvertedType.INTERVAL;
      case INT_8:
        return ConvertedType.INT_8;
      case INT_16:
        return ConvertedType.INT_16;
      case INT_32:
        return ConvertedType.INT_32;
      case INT_64:
        return ConvertedType.INT_64;
      case UINT_8:
        return ConvertedType.UINT_8;
      case UINT_16:
        return ConvertedType.UINT_16;
      case UINT_32:
        return ConvertedType.UINT_32;
      case UINT_64:
        return ConvertedType.UINT_64;
      case JSON:
        return ConvertedType.JSON;
      case BSON:
        return ConvertedType.BSON;
      default:
        throw new IllegalArgumentException("Unknown original type: " + originalType);
    }
  }

  private static void writeIntLittleEndian(PositionOutputStream stream, int value)
      throws IOException {
    stream.write(value & 0xFF);
    stream.write((value >> 8) & 0xFF);
    stream.write((value >> 16) & 0xFF);
    stream.write((value >> 24) & 0xFF);
  }

  private static org.apache.parquet.format.Encoding toFormatEncoding(Encoding encoding) {
    switch (encoding) {
      case PLAIN:
        return org.apache.parquet.format.Encoding.PLAIN;
      case PLAIN_DICTIONARY:
        return org.apache.parquet.format.Encoding.PLAIN_DICTIONARY;
      case RLE:
        return org.apache.parquet.format.Encoding.RLE;
      case BIT_PACKED:
        return org.apache.parquet.format.Encoding.BIT_PACKED;
      case DELTA_BINARY_PACKED:
        return org.apache.parquet.format.Encoding.DELTA_BINARY_PACKED;
      case DELTA_LENGTH_BYTE_ARRAY:
        return org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY;
      case DELTA_BYTE_ARRAY:
        return org.apache.parquet.format.Encoding.DELTA_BYTE_ARRAY;
      case RLE_DICTIONARY:
        return org.apache.parquet.format.Encoding.RLE_DICTIONARY;
      case BYTE_STREAM_SPLIT:
        return org.apache.parquet.format.Encoding.BYTE_STREAM_SPLIT;
      default:
        throw new IllegalArgumentException("Unknown encoding: " + encoding);
    }
  }

  private static org.apache.parquet.format.Statistics toFormatStatistics(Statistics<?> statistics) {
    if (statistics == null || !statistics.isNumNullsSet()) {
      return null;
    }

    org.apache.parquet.format.Statistics formatStats = new org.apache.parquet.format.Statistics();
    formatStats.setNull_count(statistics.getNumNulls());

    if (statistics.hasNonNullValue()) {
      byte[] min = statistics.getMinBytes();
      byte[] max = statistics.getMaxBytes();
      if (min != null && max != null) {
        formatStats.setMin_value(java.nio.ByteBuffer.wrap(min));
        formatStats.setMax_value(java.nio.ByteBuffer.wrap(max));
      }
    }

    return formatStats;
  }
}
