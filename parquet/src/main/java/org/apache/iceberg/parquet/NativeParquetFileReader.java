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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.metadata.BlockMetadata;
import org.apache.iceberg.parquet.metadata.ColumnChunkMetadata;
import org.apache.iceberg.parquet.metadata.FileMetadata;
import org.apache.iceberg.parquet.metadata.StatisticsConverter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Native Parquet file reader using Iceberg InputFile instead of Hadoop FileSystem.
 *
 * <p>Replacement for org.apache.parquet.hadoop.ParquetFileReader.
 */
@SuppressWarnings("deprecation")
class NativeParquetFileReader implements AutoCloseable {
  private static final int FOOTER_LENGTH_SIZE = 4;
  private static final int MAGIC_LENGTH = 4;
  private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};

  private final SeekableInputStream stream;
  private final org.apache.iceberg.parquet.metadata.ParquetMetadata metadata;
  private final List<BlockMetadata> rowGroups;

  private NativeParquetFileReader(
      SeekableInputStream stream, org.apache.iceberg.parquet.metadata.ParquetMetadata metadata) {
    this.stream = stream;
    this.metadata = metadata;
    this.rowGroups = metadata.blocks();
  }

  static NativeParquetFileReader open(InputFile file, org.apache.parquet.ParquetReadOptions options)
      throws IOException {
    // TODO: implement ParquetReadOptions support (page verification, crypto config, etc.)
    return open(file);
  }

  static NativeParquetFileReader open(InputFile file) throws IOException {
    SeekableInputStream stream = file.newStream();
    try {
      long fileLen = file.getLength();

      // Verify magic bytes at start
      byte[] startMagic = new byte[MAGIC_LENGTH];
      readFully(stream, startMagic);
      if (!isMagic(startMagic)) {
        throw new IOException("Not a Parquet file: " + file.location());
      }

      // Read footer length from end of file
      stream.seek(fileLen - MAGIC_LENGTH - FOOTER_LENGTH_SIZE);
      int footerLength = readIntLittleEndian(stream);

      // Read footer
      long footerStart = fileLen - MAGIC_LENGTH - FOOTER_LENGTH_SIZE - footerLength;
      stream.seek(footerStart);
      byte[] footerBytes = new byte[footerLength];
      readFully(stream, footerBytes);

      // Parse footer
      FileMetaData fileMeta = Util.readFileMetaData(new ByteArrayInputStream(footerBytes));
      org.apache.iceberg.parquet.metadata.ParquetMetadata metadata = parseMetadata(fileMeta);

      return new NativeParquetFileReader(stream, metadata);
    } catch (IOException e) {
      stream.close();
      throw e;
    }
  }

  private static org.apache.iceberg.parquet.metadata.ParquetMetadata parseMetadata(
      FileMetaData fileMeta) {
    MessageType schema = parseSchema(fileMeta.getSchema());

    Map<String, String> keyValueMetadata = Maps.newHashMap();
    if (fileMeta.isSetKey_value_metadata()) {
      for (KeyValue kv : fileMeta.getKey_value_metadata()) {
        keyValueMetadata.put(kv.getKey(), kv.getValue());
      }
    }

    FileMetadata fileMetadata =
        new FileMetadata(schema, keyValueMetadata, fileMeta.getCreated_by());

    List<BlockMetadata> blocks = Lists.newArrayList();
    if (fileMeta.isSetRow_groups()) {
      for (RowGroup rowGroup : fileMeta.getRow_groups()) {
        List<ColumnChunkMetadata> columns = Lists.newArrayList();
        for (ColumnChunk columnChunk : rowGroup.getColumns()) {
          ColumnMetaData metaData = columnChunk.getMeta_data();
          ColumnPath columnPath = getPath(metaData.getPath_in_schema());

          PrimitiveType primitiveType = schema.getType(columnPath.toArray()).asPrimitiveType();

          Set<Encoding> encodings = Sets.newHashSet();
          for (org.apache.parquet.format.Encoding enc : metaData.getEncodings()) {
            encodings.add(convertEncoding(enc));
          }

          ColumnChunkMetadata column =
              ColumnChunkMetadata.get(
                  columnPath,
                  primitiveType,
                  CompressionCodecName.fromParquet(metaData.codec),
                  convertEncodingStats(metaData.encoding_stats),
                  java.util.Collections.unmodifiableSet(encodings),
                  StatisticsConverter.fromParquetStatistics(metaData.statistics, primitiveType),
                  metaData.data_page_offset,
                  metaData.dictionary_page_offset,
                  metaData.num_values,
                  metaData.total_compressed_size,
                  metaData.total_uncompressed_size);

          columns.add(column);
        }

        blocks.add(new BlockMetadata(rowGroup.getNum_rows(), columns));
      }
    }

    return new org.apache.iceberg.parquet.metadata.ParquetMetadata(fileMetadata, blocks);
  }

  private static MessageType parseSchema(List<SchemaElement> schema) {
    java.util.Iterator<SchemaElement> schemaIterator = schema.iterator();
    SchemaElement rootSchema = schemaIterator.next();
    Types.MessageTypeBuilder builder = Types.buildMessage();
    readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
    return builder.named(rootSchema.name);
  }

  private static void readTypeSchema(
      Types.GroupBuilder<?> builder,
      java.util.Iterator<SchemaElement> schemaIterator,
      int typeCount) {
    for (int i = 0; i < typeCount; i++) {
      SchemaElement element = schemaIterator.next();
      Types.Builder<?, ?> typeBuilder;
      if (element.type == null) {
        typeBuilder = builder.group(Type.Repetition.valueOf(element.repetition_type.name()));
        readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
      } else {
        Types.PrimitiveBuilder<?> primitiveBuilder =
            builder.primitive(
                getPrimitive(element.type),
                Type.Repetition.valueOf(element.repetition_type.name()));
        if (element.isSetType_length()) {
          primitiveBuilder.length(element.type_length);
        }
        if (element.isSetPrecision()) {
          primitiveBuilder.precision(element.precision);
        }
        if (element.isSetScale()) {
          primitiveBuilder.scale(element.scale);
        }
        typeBuilder = primitiveBuilder;
      }

      // Add logical type annotations
      if (element.isSetLogicalType()) {
        typeBuilder.as(getLogicalTypeAnnotation(element.logicalType));
      } else if (element.isSetConverted_type()) {
        typeBuilder.as(getLogicalTypeAnnotation(element.converted_type));
      }

      if (element.isSetField_id()) {
        typeBuilder.id(element.field_id);
      }
      typeBuilder.named(element.name.toLowerCase(java.util.Locale.ENGLISH));
    }
  }

  private static PrimitiveTypeName getPrimitive(org.apache.parquet.format.Type type) {
    switch (type) {
      case BOOLEAN:
        return PrimitiveTypeName.BOOLEAN;
      case INT32:
        return PrimitiveTypeName.INT32;
      case INT64:
        return PrimitiveTypeName.INT64;
      case INT96:
        return PrimitiveTypeName.INT96;
      case FLOAT:
        return PrimitiveTypeName.FLOAT;
      case DOUBLE:
        return PrimitiveTypeName.DOUBLE;
      case BYTE_ARRAY:
        return PrimitiveTypeName.BINARY;
      case FIXED_LEN_BYTE_ARRAY:
        return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  private static ColumnPath getPath(List<String> pathInSchema) {
    String[] path =
        pathInSchema.stream()
            .map(value -> value.toLowerCase(java.util.Locale.ENGLISH))
            .toArray(String[]::new);
    return ColumnPath.get(path);
  }

  /**
   * Get Parquet footer metadata.
   *
   * @return ParquetMetadata
   */
  public org.apache.iceberg.parquet.metadata.ParquetMetadata footer() {
    return metadata;
  }

  /**
   * Get file schema.
   *
   * @return MessageType schema
   */
  public MessageType fileSchema() {
    return metadata.fileMetaData().schema();
  }

  /**
   * Get list of row groups.
   *
   * @return list of BlockMetadata
   */
  public List<BlockMetadata> rowGroups() {
    return rowGroups;
  }

  /**
   * Get full Parquet metadata.
   *
   * @return ParquetMetadata
   */
  public org.apache.iceberg.parquet.metadata.ParquetMetadata metadata() {
    return metadata;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  private static boolean isMagic(byte[] magic) {
    if (magic.length != MAGIC_LENGTH) {
      return false;
    }
    for (int i = 0; i < MAGIC_LENGTH; i++) {
      if (magic[i] != MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  private static void readFully(SeekableInputStream stream, byte[] buffer) throws IOException {
    int offset = 0;
    int remaining = buffer.length;
    while (remaining > 0) {
      int bytesRead = stream.read(buffer, offset, remaining);
      if (bytesRead < 0) {
        throw new IOException("Unexpected end of stream");
      }
      offset += bytesRead;
      remaining -= bytesRead;
    }
  }

  private static int readIntLittleEndian(SeekableInputStream stream) throws IOException {
    int b0 = stream.read() & 0xFF;
    int b1 = stream.read() & 0xFF;
    int b2 = stream.read() & 0xFF;
    int b3 = stream.read() & 0xFF;
    return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
  }

  /** Convert format.Encoding to column.Encoding. */
  private static Encoding convertEncoding(org.apache.parquet.format.Encoding encoding) {
    switch (encoding) {
      case PLAIN:
        return Encoding.PLAIN;
      case PLAIN_DICTIONARY:
        return Encoding.PLAIN_DICTIONARY;
      case RLE:
        return Encoding.RLE;
      case BIT_PACKED:
        return Encoding.BIT_PACKED;
      case DELTA_BINARY_PACKED:
        return Encoding.DELTA_BINARY_PACKED;
      case DELTA_LENGTH_BYTE_ARRAY:
        return Encoding.DELTA_LENGTH_BYTE_ARRAY;
      case DELTA_BYTE_ARRAY:
        return Encoding.DELTA_BYTE_ARRAY;
      case RLE_DICTIONARY:
        return Encoding.RLE_DICTIONARY;
      case BYTE_STREAM_SPLIT:
        return Encoding.BYTE_STREAM_SPLIT;
      default:
        throw new IllegalArgumentException("Unknown encoding: " + encoding);
    }
  }

  /** Convert format PageEncodingStats list to column.EncodingStats. */
  private static EncodingStats convertEncodingStats(
      java.util.List<org.apache.parquet.format.PageEncodingStats> pageEncodingStats) {
    if (pageEncodingStats == null || pageEncodingStats.isEmpty()) {
      return null;
    }

    EncodingStats.Builder builder = new EncodingStats.Builder();
    for (org.apache.parquet.format.PageEncodingStats stat : pageEncodingStats) {
      if (stat.isSetPage_type() && stat.isSetEncoding() && stat.isSetCount()) {
        Encoding encoding = convertEncoding(stat.getEncoding());

        switch (stat.getPage_type()) {
          case DATA_PAGE:
          case DATA_PAGE_V2:
            builder.addDataEncoding(encoding);
            break;
          case DICTIONARY_PAGE:
            builder.addDictEncoding(encoding);
            break;
          default:
            break;
        }
      }
    }

    return builder.build();
  }

  private static LogicalTypeAnnotation getLogicalTypeAnnotation(
      org.apache.parquet.format.LogicalType type) {
    switch (type.getSetField()) {
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case TIME:
        org.apache.parquet.format.TimeType time = type.getTIME();
        return LogicalTypeAnnotation.timeType(time.isAdjustedToUTC, convertTimeUnit(time.unit));
      case STRING:
        return LogicalTypeAnnotation.stringType();
      case DECIMAL:
        org.apache.parquet.format.DecimalType decimal = type.getDECIMAL();
        return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
      case INTEGER:
        org.apache.parquet.format.IntType integer = type.getINTEGER();
        return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
      case UNKNOWN:
        return LogicalTypeAnnotation.unknownType();
      case TIMESTAMP:
        org.apache.parquet.format.TimestampType timestamp = type.getTIMESTAMP();
        return LogicalTypeAnnotation.timestampType(
            timestamp.isAdjustedToUTC, convertTimeUnit(timestamp.unit));
      case UUID:
        return LogicalTypeAnnotation.uuidType();
      case FLOAT16:
        return LogicalTypeAnnotation.float16Type();
      case VARIANT:
        org.apache.parquet.format.VariantType variant = type.getVARIANT();
        return LogicalTypeAnnotation.variantType(variant.specification_version);
      default:
        throw new IllegalArgumentException("Unknown logical type: " + type);
    }
  }

  private static LogicalTypeAnnotation.TimeUnit convertTimeUnit(
      org.apache.parquet.format.TimeUnit unit) {
    switch (unit.getSetField()) {
      case MILLIS:
        return LogicalTypeAnnotation.TimeUnit.MILLIS;
      case MICROS:
        return LogicalTypeAnnotation.TimeUnit.MICROS;
      case NANOS:
        return LogicalTypeAnnotation.TimeUnit.NANOS;
      default:
        throw new IllegalArgumentException("Unknown time unit: " + unit);
    }
  }

  private static LogicalTypeAnnotation getLogicalTypeAnnotation(ConvertedType convertedType) {
    return LogicalTypeAnnotation.fromOriginalType(OriginalType.valueOf(convertedType.name()), null);
  }
}
