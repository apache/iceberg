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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.metadata.BlockMetadata;
import org.apache.iceberg.parquet.metadata.ColumnChunkExtras;
import org.apache.iceberg.parquet.metadata.ColumnChunkMetadata;
import org.apache.iceberg.parquet.metadata.FileMetadata;
import org.apache.iceberg.parquet.metadata.StatisticsConverter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.crypto.AesCipher;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.crypto.ModuleCipherFactory.ModuleType;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TagVerificationException;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.BloomFilterHeader;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnCryptoMetaData;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.EncryptionWithColumnKey;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.DecimalMetadata;
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
  private static final int COPY_BUFFER_SIZE = 64 * 1024;
  private static final byte[] MAGIC = new byte[] {'P', 'A', 'R', '1'};
  private static final byte[] ENCRYPTED_MAGIC = new byte[] {'P', 'A', 'R', 'E'};

  private final SeekableInputStream stream;
  private final org.apache.iceberg.parquet.metadata.ParquetMetadata metadata;
  private final List<BlockMetadata> rowGroups;
  private final org.apache.parquet.ParquetReadOptions options;
  private final InternalFileDecryptor fileDecryptor;
  private CompressionCodecFactory codecFactory;
  private int currentRowGroup = 0;

  private NativeParquetFileReader(
      SeekableInputStream stream,
      org.apache.iceberg.parquet.metadata.ParquetMetadata metadata,
      org.apache.parquet.ParquetReadOptions options,
      InternalFileDecryptor fileDecryptor) {
    this.fileDecryptor = fileDecryptor;
    this.stream = stream;
    this.metadata = metadata;
    this.rowGroups = metadata.blocks();
    this.options = options;
  }

  static NativeParquetFileReader open(InputFile file) throws IOException {
    return open(file, null);
  }

  static NativeParquetFileReader open(InputFile file, org.apache.parquet.ParquetReadOptions options)
      throws IOException {
    SeekableInputStream stream = file.newStream();
    try {
      RawFooter footer = readRawFooter(stream, file);
      FileDecryptionProperties decryptionProperties =
          options == null ? null : options.getDecryptionProperties();
      InternalFileDecryptor decryptor =
          decryptionProperties == null ? null : new InternalFileDecryptor(decryptionProperties);
      FileMetaData fileMeta = readFooter(footer, decryptor);
      org.apache.iceberg.parquet.metadata.ParquetMetadata metadata =
          parseMetadata(fileMeta, decryptor, footer.encrypted());
      boolean plaintextFile = decryptor != null && decryptor.plaintextFile();
      return new NativeParquetFileReader(
          stream, metadata, options, plaintextFile ? null : decryptor);
    } catch (IOException | RuntimeException e) {
      stream.close();
      throw e;
    }
  }

  private record RawFooter(byte[] bytes, boolean encrypted) {}

  private static RawFooter readRawFooter(SeekableInputStream stream, InputFile file)
      throws IOException {
    long fileLen = file.getLength();
    long trailerLen = FOOTER_LENGTH_SIZE + MAGIC_LENGTH;
    if (fileLen < MAGIC_LENGTH + trailerLen) {
      throw new IOException("Not a Parquet file (too small: " + fileLen + "): " + file.location());
    }

    byte[] startMagic = new byte[MAGIC_LENGTH];
    readFully(stream, startMagic);
    if (!isMagic(startMagic) && !isEncryptedMagic(startMagic)) {
      throw new IOException("Not a Parquet file: " + file.location());
    }

    stream.seek(fileLen - trailerLen);
    byte[] trailer = new byte[(int) trailerLen];
    readFully(stream, trailer);
    byte[] endMagic = java.util.Arrays.copyOfRange(trailer, FOOTER_LENGTH_SIZE, trailer.length);
    boolean encrypted = isEncryptedMagic(endMagic);
    if (!encrypted && !isMagic(endMagic)) {
      throw new IOException("Not a Parquet file (bad end magic): " + file.location());
    }

    int footerLength =
        ByteBuffer.wrap(trailer, 0, FOOTER_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN).getInt();
    long footerStart = fileLen - trailerLen - footerLength;
    if (footerLength < 0 || footerStart < MAGIC_LENGTH) {
      throw new IOException(
          "Invalid footer length " + footerLength + " in file: " + file.location());
    }

    stream.seek(footerStart);
    byte[] footerBytes = new byte[footerLength];
    readFully(stream, footerBytes);
    return new RawFooter(footerBytes, encrypted);
  }

  private static FileMetaData readFooter(RawFooter footer, InternalFileDecryptor decryptor)
      throws IOException {
    ByteArrayInputStream footerStream = new ByteArrayInputStream(footer.bytes());
    if (footer.encrypted()) {
      if (decryptor == null) {
        throw new ParquetCryptoRuntimeException(
            "Trying to read file with encrypted footer. No keys available");
      }

      FileCryptoMetaData cryptoMetaData = Util.readFileCryptoMetaData(footerStream);
      decryptor.setFileCryptoMetaData(
          cryptoMetaData.getEncryption_algorithm(), true, cryptoMetaData.getKey_metadata());
      return Util.readFileMetaData(
          footerStream,
          decryptor.fetchFooterDecryptor(),
          AesCipher.createFooterAAD(decryptor.getFileAAD()));
    }

    FileMetaData fileMeta = Util.readFileMetaData(footerStream);
    if (decryptor != null) {
      initPlaintextFooterDecryptor(fileMeta, footer.bytes(), decryptor);
    }

    return fileMeta;
  }

  private static void initPlaintextFooterDecryptor(
      FileMetaData fileMeta, byte[] footerBytes, InternalFileDecryptor decryptor) {
    if (!fileMeta.isSetEncryption_algorithm()) {
      decryptor.setPlaintextFile();
      if (!decryptor.plaintextFilesAllowed()) {
        throw new ParquetCryptoRuntimeException("Applying decryptor on plaintext file");
      }

      return;
    }

    decryptor.setFileCryptoMetaData(
        fileMeta.getEncryption_algorithm(), false, fileMeta.getFooter_signing_key_metadata());
    if (decryptor.checkFooterIntegrity()) {
      verifyFooterIntegrity(footerBytes, decryptor);
    }
  }

  static org.apache.iceberg.parquet.metadata.ParquetMetadata parseMetadata(FileMetaData fileMeta) {
    return parseMetadata(fileMeta, null, false);
  }

  static org.apache.iceberg.parquet.metadata.ParquetMetadata parseMetadata(
      FileMetaData fileMeta, InternalFileDecryptor decryptor, boolean encryptedFooter) {
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
        int columnOrdinal = 0;
        for (ColumnChunk columnChunk : rowGroup.getColumns()) {
          columns.add(
              parseColumn(
                  columnChunk, rowGroup, schema, decryptor, encryptedFooter, columnOrdinal++));
        }

        blocks.add(
            new BlockMetadata(
                rowGroup.getNum_rows(),
                rowGroup.getTotal_byte_size(),
                rowGroup.isSetOrdinal() ? rowGroup.getOrdinal() : -1,
                columns,
                rowGroup.isSetSorting_columns() ? rowGroup.getSorting_columns() : null));
      }
    }

    return new org.apache.iceberg.parquet.metadata.ParquetMetadata(fileMetadata, blocks);
  }

  private static ColumnChunkMetadata parseColumn(
      ColumnChunk columnChunk,
      RowGroup rowGroup,
      MessageType schema,
      InternalFileDecryptor decryptor,
      boolean encryptedFooter,
      int columnOrdinal) {
    ColumnMetaData metaData =
        resolveColumnMetaData(columnChunk, rowGroup, decryptor, encryptedFooter, columnOrdinal);
    ColumnPath columnPath = getPath(metaData.getPath_in_schema());
    PrimitiveType primitiveType = schema.getType(columnPath.toArray()).asPrimitiveType();

    Set<Encoding> encodings = Sets.newHashSet();
    for (org.apache.parquet.format.Encoding enc : metaData.getEncodings()) {
      encodings.add(convertEncoding(enc));
    }

    return ColumnChunkMetadata.get(
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
            metaData.total_uncompressed_size)
        .withExtras(extras(columnChunk, metaData))
        .withRowGroupOrdinal(rowGroup.isSetOrdinal() ? rowGroup.getOrdinal() : -1);
  }

  private static ColumnMetaData resolveColumnMetaData(
      ColumnChunk columnChunk,
      RowGroup rowGroup,
      InternalFileDecryptor decryptor,
      boolean encryptedFooter,
      int columnOrdinal) {
    ColumnCryptoMetaData cryptoMetaData = columnChunk.getCrypto_metadata();
    if (cryptoMetaData != null) {
      return decryptColumnMetaData(
          columnChunk, rowGroup, cryptoMetaData, decryptor, encryptedFooter, columnOrdinal);
    }

    ColumnMetaData metaData = columnChunk.getMeta_data();
    if (decryptor != null && !decryptor.plaintextFile()) {
      decryptor.setColumnCryptoMetadata(
          getPath(metaData.getPath_in_schema()), false, false, null, columnOrdinal);
    }

    return metaData;
  }

  private static ColumnMetaData decryptColumnMetaData(
      ColumnChunk columnChunk,
      RowGroup rowGroup,
      ColumnCryptoMetaData cryptoMetaData,
      InternalFileDecryptor decryptor,
      boolean encryptedFooter,
      int columnOrdinal) {
    if (decryptor == null) {
      throw new ParquetCryptoRuntimeException("Column is encrypted. No keys available");
    }

    ColumnMetaData metaData = columnChunk.getMeta_data();
    boolean footerKey = cryptoMetaData.isSetENCRYPTION_WITH_FOOTER_KEY();
    if (footerKey) {
      ColumnPath path = getPath(metaData.getPath_in_schema());
      if (!encryptedFooter) {
        metaData =
            readEncryptedMetaData(
                columnChunk, rowGroup, decryptor.fetchFooterDecryptor(), decryptor, columnOrdinal);
      }

      decryptor.setColumnCryptoMetadata(path, true, true, null, columnOrdinal);
      return metaData;
    }

    EncryptionWithColumnKey columnKey = cryptoMetaData.getENCRYPTION_WITH_COLUMN_KEY();
    ColumnPath path = ColumnPath.get(columnKey.getPath_in_schema().toArray(new String[0]));
    InternalColumnDecryptionSetup setup =
        decryptor.setColumnCryptoMetadata(
            path, true, false, columnKey.getKey_metadata(), columnOrdinal);
    return readEncryptedMetaData(
        columnChunk, rowGroup, setup.getMetaDataDecryptor(), decryptor, columnOrdinal);
  }

  private static ColumnMetaData readEncryptedMetaData(
      ColumnChunk columnChunk,
      RowGroup rowGroup,
      BlockCipher.Decryptor metaDataDecryptor,
      InternalFileDecryptor decryptor,
      int columnOrdinal) {
    try {
      return Util.readColumnMetaData(
          new ByteArrayInputStream(columnChunk.getEncrypted_column_metadata()),
          metaDataDecryptor,
          AesCipher.createModuleAAD(
              decryptor.getFileAAD(),
              ModuleType.ColumnMetaData,
              rowGroup.getOrdinal(),
              columnOrdinal,
              -1));
    } catch (IOException e) {
      throw new ParquetCryptoRuntimeException("Failed to decrypt column metadata", e);
    }
  }

  private static void verifyFooterIntegrity(byte[] footerBytes, InternalFileDecryptor decryptor) {
    int signatureLength = AesCipher.NONCE_LENGTH + AesCipher.GCM_TAG_LENGTH;
    byte[] nonce =
        java.util.Arrays.copyOfRange(
            footerBytes,
            footerBytes.length - signatureLength,
            footerBytes.length - AesCipher.GCM_TAG_LENGTH);
    byte[] tag =
        java.util.Arrays.copyOfRange(
            footerBytes, footerBytes.length - AesCipher.GCM_TAG_LENGTH, footerBytes.length);
    byte[] serialized = java.util.Arrays.copyOf(footerBytes, footerBytes.length - signatureLength);
    byte[] encrypted =
        decryptor
            .createSignedFooterEncryptor()
            .encrypt(false, serialized, nonce, AesCipher.createFooterAAD(decryptor.getFileAAD()));
    byte[] calculated =
        java.util.Arrays.copyOfRange(
            encrypted, encrypted.length - AesCipher.GCM_TAG_LENGTH, encrypted.length);
    if (!java.util.Arrays.equals(tag, calculated)) {
      throw new TagVerificationException("Signature mismatch in plaintext footer");
    }
  }

  private static ColumnChunkExtras extras(ColumnChunk chunk, ColumnMetaData columnMeta) {
    return new ColumnChunkExtras(
        columnMeta.isSetBloom_filter_offset() ? columnMeta.getBloom_filter_offset() : -1,
        columnMeta.isSetBloom_filter_length() ? columnMeta.getBloom_filter_length() : -1,
        chunk.isSetColumn_index_offset() ? chunk.getColumn_index_offset() : -1,
        chunk.isSetColumn_index_length() ? chunk.getColumn_index_length() : -1,
        chunk.isSetOffset_index_offset() ? chunk.getOffset_index_offset() : -1,
        chunk.isSetOffset_index_length() ? chunk.getOffset_index_length() : -1,
        columnMeta.isSetSize_statistics() ? columnMeta.getSize_statistics() : null,
        columnMeta.isSetGeospatial_statistics() ? columnMeta.getGeospatial_statistics() : null,
        columnMeta.isSetKey_value_metadata() ? columnMeta.getKey_value_metadata() : null);
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
        typeBuilder.as(getLogicalTypeAnnotation(element.converted_type, element));
      }

      if (element.isSetField_id()) {
        typeBuilder.id(element.field_id);
      }
      typeBuilder.named(element.name);
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
    return ColumnPath.get(pathInSchema.toArray(new String[0]));
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

  /**
   * Returns the row groups whose first byte falls in [start, end), matching Hadoop's split-range
   * semantics.
   */
  public List<BlockMetadata> rowGroupsInRange(long start, long end) {
    List<BlockMetadata> result = Lists.newArrayList();
    for (BlockMetadata block : rowGroups) {
      long startPos = block.columns().get(0).getStartingPos();
      if (startPos >= start && startPos < end) {
        result.add(block);
      }
    }
    return result;
  }

  /** Reads the next row group in file order, or returns null once all have been read. */
  public PageReadStore readNextRowGroup() throws IOException {
    if (currentRowGroup >= rowGroups.size()) {
      return null;
    }
    return readRowGroup(rowGroups.get(currentRowGroup++));
  }

  /** Skips the next row group without reading it. */
  public void skipNextRowGroup() {
    currentRowGroup++;
  }

  /** Reads and decompresses all pages of the given row group. */
  public PageReadStore readRowGroup(BlockMetadata block) throws IOException {
    Map<ColumnPath, ColumnChunkPages> pages = Maps.newHashMap();
    for (ColumnChunkMetadata column : block.columns()) {
      pages.put(column.path(), readColumnChunk(column));
    }

    return new NativePageReadStore(block.rowCount(), pages);
  }

  private ColumnChunkPages readColumnChunk(ColumnChunkMetadata column) throws IOException {
    long length = column.totalSize();
    Preconditions.checkState(
        length >= 0 && length <= Integer.MAX_VALUE,
        "Column chunk too large for in-memory read: %s bytes",
        length);
    stream.seek(column.getStartingPos());
    byte[] chunk = new byte[(int) length];
    readFully(stream, chunk);

    return new ChunkPageReader(column, new ByteArrayInputStream(chunk)).readAll();
  }

  private final class ChunkPageReader {
    private final ColumnChunkMetadata column;
    private final ByteArrayInputStream in;
    private final ColumnDescriptor descriptor;
    private final CompressionCodecFactory.BytesInputDecompressor decompressor;
    private final InternalColumnDecryptionSetup crypto;
    private final List<DataPage> dataPages = Lists.newArrayList();
    private DictionaryPage dictionaryPage;
    private long valuesRead;
    private long rowsBefore;
    private int dataPageOrdinal;

    ChunkPageReader(ColumnChunkMetadata column, ByteArrayInputStream in) {
      this.column = column;
      this.in = in;
      this.descriptor = fileSchema().getColumnDescription(column.path().toArray());
      this.decompressor = codecs().getDecompressor(column.codec());
      this.crypto = columnCrypto(column);
    }

    ColumnChunkPages readAll() throws IOException {
      while (valuesRead < column.valueCount() && in.available() > 0) {
        PageHeader header = readHeader();
        byte[] payload = readPayload(header);
        switch (header.type) {
          case DICTIONARY_PAGE -> addDictionary(header, payload);
          case DATA_PAGE -> addDataPageV1(header, payload);
          case DATA_PAGE_V2 -> addDataPageV2(header, payload);
          default -> {
            // index and unknown page types carry no values
          }
        }
      }

      return new ColumnChunkPages(dictionaryPage, dataPages, column.valueCount());
    }

    private PageHeader readHeader() throws IOException {
      if (crypto == null) {
        return Util.readPageHeader(in);
      }

      boolean dictionaryNext = dictionaryPage == null && hasDictionary(column);
      ModuleType type =
          dictionaryNext ? ModuleType.DictionaryPageHeader : ModuleType.DataPageHeader;
      return Util.readPageHeader(
          in, crypto.getMetaDataDecryptor(), pageAad(type, dictionaryNext ? -1 : dataPageOrdinal));
    }

    private byte[] readPayload(PageHeader header) throws IOException {
      int size = header.getCompressed_page_size();
      byte[] payload = in.readNBytes(size);
      if (payload.length != size) {
        throw new IOException("Unexpected end of column chunk for " + column.path());
      }

      if (options != null && options.usePageChecksumVerification() && header.isSetCrc()) {
        CRC32 crc = new CRC32();
        crc.update(payload, 0, payload.length);
        if ((int) crc.getValue() != header.getCrc()) {
          throw new IOException("CRC mismatch for page in column " + column.path());
        }
      }

      if (crypto == null || header.type == PageType.DATA_PAGE_V2) {
        return payload;
      }

      boolean dictionary = header.type == PageType.DICTIONARY_PAGE;
      return crypto
          .getDataDecryptor()
          .decrypt(
              payload,
              pageAad(
                  dictionary ? ModuleType.DictionaryPage : ModuleType.DataPage,
                  dictionary ? -1 : dataPageOrdinal));
    }

    private byte[] pageAad(ModuleType type, int pageOrdinal) {
      return AesCipher.createModuleAAD(
          fileDecryptor.getFileAAD(),
          type,
          column.rowGroupOrdinal(),
          crypto.getOrdinal(),
          pageOrdinal);
    }

    private void addDictionary(PageHeader header, byte[] payload) throws IOException {
      Preconditions.checkState(
          dictionaryPage == null, "More than one dictionary page in %s", column.path());
      org.apache.parquet.format.DictionaryPageHeader dictHeader =
          header.getDictionary_page_header();
      BytesInput bytes =
          decompressor.decompress(BytesInput.from(payload), header.getUncompressed_page_size());
      this.dictionaryPage =
          new DictionaryPage(
              BytesInput.copy(bytes),
              header.getUncompressed_page_size(),
              dictHeader.getNum_values(),
              convertEncoding(dictHeader.getEncoding()));
    }

    private void addDataPageV1(PageHeader header, byte[] payload) throws IOException {
      org.apache.parquet.format.DataPageHeader dataHeader = header.getData_page_header();
      BytesInput bytes =
          decompressor.decompress(BytesInput.from(payload), header.getUncompressed_page_size());
      dataPages.add(
          new DataPageV1(
              BytesInput.copy(bytes),
              dataHeader.getNum_values(),
              header.getUncompressed_page_size(),
              StatisticsConverter.fromParquetStatistics(
                  dataHeader.getStatistics(), descriptor.getPrimitiveType()),
              convertEncoding(dataHeader.getRepetition_level_encoding()),
              convertEncoding(dataHeader.getDefinition_level_encoding()),
              convertEncoding(dataHeader.getEncoding())));
      valuesRead += dataHeader.getNum_values();
      dataPageOrdinal++;
    }

    private void addDataPageV2(PageHeader header, byte[] encryptedPayload) throws IOException {
      org.apache.parquet.format.DataPageHeaderV2 v2 = header.getData_page_header_v2();
      int repetitionLength = v2.getRepetition_levels_byte_length();
      int levelsLength = repetitionLength + v2.getDefinition_levels_byte_length();
      byte[] payload =
          crypto == null ? encryptedPayload : decryptValues(encryptedPayload, levelsLength);

      BytesInput repetition = BytesInput.from(payload, 0, repetitionLength);
      BytesInput definition =
          BytesInput.from(payload, repetitionLength, v2.getDefinition_levels_byte_length());
      BytesInput values = BytesInput.from(payload, levelsLength, payload.length - levelsLength);
      if (v2.isIs_compressed() && payload.length > levelsLength) {
        values =
            BytesInput.copy(
                decompressor.decompress(values, header.getUncompressed_page_size() - levelsLength));
      }

      dataPages.add(
          DataPageV2.uncompressed(
              v2.getNum_rows(),
              v2.getNum_nulls(),
              v2.getNum_values(),
              rowsBefore,
              BytesInput.copy(repetition),
              BytesInput.copy(definition),
              convertEncoding(v2.getEncoding()),
              values,
              StatisticsConverter.fromParquetStatistics(
                  v2.getStatistics(), descriptor.getPrimitiveType())));
      rowsBefore += v2.getNum_rows();
      valuesRead += v2.getNum_values();
      dataPageOrdinal++;
    }

    private byte[] decryptValues(byte[] payload, int levelsLength) {
      byte[] values =
          crypto
              .getDataDecryptor()
              .decrypt(
                  java.util.Arrays.copyOfRange(payload, levelsLength, payload.length),
                  pageAad(ModuleType.DataPage, dataPageOrdinal));
      byte[] combined = new byte[levelsLength + values.length];
      System.arraycopy(payload, 0, combined, 0, levelsLength);
      System.arraycopy(values, 0, combined, levelsLength, values.length);
      return combined;
    }
  }

  private InternalColumnDecryptionSetup columnCrypto(ColumnChunkMetadata column) {
    if (fileDecryptor == null) {
      return null;
    }

    InternalColumnDecryptionSetup setup = fileDecryptor.getColumnSetup(column.path());
    return setup.isEncrypted() ? setup : null;
  }

  private byte[] moduleAad(
      ColumnChunkMetadata column, InternalColumnDecryptionSetup setup, ModuleType type) {
    return AesCipher.createModuleAAD(
        fileDecryptor.getFileAAD(), type, column.rowGroupOrdinal(), setup.getOrdinal(), -1);
  }

  private static boolean hasDictionary(ColumnChunkMetadata column) {
    return column.dictionaryPageOffset() > 0
        || (column.encodingStats() != null && column.encodingStats().hasDictionaryPages());
  }

  private CompressionCodecFactory codecs() {
    if (codecFactory == null) {
      codecFactory =
          options != null && options.getCodecFactory() != null
              ? options.getCodecFactory()
              : new NativeCodecFactory();
    }
    return codecFactory;
  }

  /** Reads the dictionary page of a column chunk, or returns null if it has none. */
  public DictionaryPage readDictionary(ColumnChunkMetadata column) throws IOException {
    if (!column.encodingStats().hasDictionaryPages() && column.dictionaryPageOffset() <= 0) {
      return null;
    }

    return readColumnChunk(column).dictionary;
  }

  /** Reads the column index of a column chunk, or returns null if it has none. */
  public org.apache.parquet.format.ColumnIndex readColumnIndex(ColumnChunkMetadata column)
      throws IOException {
    ColumnChunkExtras extras = column.extras();
    if (!extras.hasColumnIndex()) {
      return null;
    }

    stream.seek(extras.columnIndexOffset());
    ByteArrayInputStream in = new ByteArrayInputStream(readBytes(extras.columnIndexLength()));
    InternalColumnDecryptionSetup setup = columnCrypto(column);
    if (setup == null) {
      return Util.readColumnIndex(in);
    }

    return Util.readColumnIndex(
        in, setup.getMetaDataDecryptor(), moduleAad(column, setup, ModuleType.ColumnIndex));
  }

  /** Reads the offset index of a column chunk, or returns null if it has none. */
  public org.apache.parquet.format.OffsetIndex readOffsetIndex(ColumnChunkMetadata column)
      throws IOException {
    ColumnChunkExtras extras = column.extras();
    if (!extras.hasOffsetIndex()) {
      return null;
    }

    stream.seek(extras.offsetIndexOffset());
    ByteArrayInputStream in = new ByteArrayInputStream(readBytes(extras.offsetIndexLength()));
    InternalColumnDecryptionSetup setup = columnCrypto(column);
    if (setup == null) {
      return Util.readOffsetIndex(in);
    }

    return Util.readOffsetIndex(
        in, setup.getMetaDataDecryptor(), moduleAad(column, setup, ModuleType.OffsetIndex));
  }

  /** Reads the bloom filter of a column chunk, or returns null if it has none. */
  public BloomFilter readBloomFilter(ColumnChunkMetadata column) throws IOException {
    ColumnChunkExtras extras = column.extras();
    if (extras.bloomFilterOffset() < 0) {
      return null;
    }

    stream.seek(extras.bloomFilterOffset());
    InternalColumnDecryptionSetup setup = columnCrypto(column);
    BloomFilterHeader header =
        setup == null
            ? Util.readBloomFilterHeader(stream)
            : Util.readBloomFilterHeader(
                stream,
                setup.getMetaDataDecryptor(),
                moduleAad(column, setup, ModuleType.BloomFilterHeader));
    if (!header.getAlgorithm().isSetBLOCK()
        || !header.getHash().isSetXXHASH()
        || !header.getCompression().isSetUNCOMPRESSED()) {
      return null;
    }

    byte[] bitset;
    if (setup == null) {
      bitset = new byte[header.getNumBytes()];
      readFully(stream, bitset);
    } else {
      bitset =
          setup
              .getMetaDataDecryptor()
              .decrypt(stream, moduleAad(column, setup, ModuleType.BloomFilterBitset));
      if (bitset.length != header.getNumBytes()) {
        throw new ParquetCryptoRuntimeException("Wrong length of decrypted bloom filter bitset");
      }
    }

    return new BlockSplitBloomFilter(bitset);
  }

  /** Copies a byte range of the file to the output without decoding it. */
  void copyRange(long start, long length, OutputStream out) throws IOException {
    stream.seek(start);
    byte[] buffer = new byte[(int) Math.min(COPY_BUFFER_SIZE, Math.max(length, 1))];
    long remaining = length;
    while (remaining > 0) {
      int bytesRead = stream.read(buffer, 0, (int) Math.min(buffer.length, remaining));
      if (bytesRead < 0) {
        throw new IOException("Unexpected end of stream");
      }
      out.write(buffer, 0, bytesRead);
      remaining -= bytesRead;
    }
  }

  private byte[] readBytes(int length) throws IOException {
    byte[] bytes = new byte[length];
    readFully(stream, bytes);
    return bytes;
  }

  private static final class ColumnChunkPages {
    private final DictionaryPage dictionary;
    private final List<DataPage> pages;
    private final long valueCount;

    ColumnChunkPages(DictionaryPage dictionary, List<DataPage> pages, long valueCount) {
      this.dictionary = dictionary;
      this.pages = pages;
      this.valueCount = valueCount;
    }
  }

  private static final class NativePageReadStore implements PageReadStore {
    private final long rowCount;
    private final Map<ColumnPath, ColumnChunkPages> chunks;

    NativePageReadStore(long rowCount, Map<ColumnPath, ColumnChunkPages> chunks) {
      this.rowCount = rowCount;
      this.chunks = chunks;
    }

    @Override
    public PageReader getPageReader(ColumnDescriptor descriptor) {
      ColumnChunkPages chunk = chunks.get(ColumnPath.get(descriptor.getPath()));
      if (chunk == null) {
        throw new IllegalArgumentException("Column not in row group: " + descriptor);
      }

      return new PageReader() {
        private final Iterator<DataPage> iterator = chunk.pages.iterator();

        @Override
        public DictionaryPage readDictionaryPage() {
          return chunk.dictionary;
        }

        @Override
        public long getTotalValueCount() {
          return chunk.valueCount;
        }

        @Override
        public DataPage readPage() {
          return iterator.hasNext() ? iterator.next() : null;
        }
      };
    }

    @Override
    public long getRowCount() {
      return rowCount;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (codecFactory != null) {
        codecFactory.release();
      }
    } finally {
      stream.close();
    }
  }

  private static boolean isEncryptedMagic(byte[] magic) {
    return java.util.Arrays.equals(magic, ENCRYPTED_MAGIC);
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
            builder.addDataEncoding(encoding, stat.getCount());
            break;
          case DATA_PAGE_V2:
            builder.withV2Pages();
            builder.addDataEncoding(encoding, stat.getCount());
            break;
          case DICTIONARY_PAGE:
            builder.addDictEncoding(encoding, stat.getCount());
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
      case GEOMETRY:
        return LogicalTypeAnnotation.geometryType(type.getGEOMETRY().getCrs());
      case GEOGRAPHY:
        org.apache.parquet.format.GeographyType geography = type.getGEOGRAPHY();
        return geography.isSetAlgorithm()
            ? LogicalTypeAnnotation.geographyType(
                geography.getCrs(),
                org.apache.parquet.column.schema.EdgeInterpolationAlgorithm.valueOf(
                    geography.getAlgorithm().name()))
            : LogicalTypeAnnotation.geographyType(geography.getCrs(), null);
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

  private static LogicalTypeAnnotation getLogicalTypeAnnotation(
      ConvertedType convertedType, SchemaElement element) {
    DecimalMetadata decimal =
        convertedType == ConvertedType.DECIMAL
            ? new DecimalMetadata(element.getPrecision(), element.getScale())
            : null;
    return LogicalTypeAnnotation.fromOriginalType(
        OriginalType.valueOf(convertedType.name()), decimal);
  }
}
