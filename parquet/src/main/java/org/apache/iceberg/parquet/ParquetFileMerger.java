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

import static java.util.Collections.emptyMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Utility class for performing strict schema validation and merging of Parquet files at the
 * row-group level.
 *
 * <p>This class ensures that all input files have identical Parquet schemas before merging. The
 * merge operation is performed by copying row groups directly without
 * serialization/deserialization, providing significant performance benefits over traditional
 * read-rewrite approaches.
 *
 * <p>This class works with any Iceberg FileIO implementation (HadoopFileIO, S3FileIO, GCSFileIO,
 * etc.), making it cloud-agnostic.
 *
 * <p>TODO: Encrypted tables are not supported
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Row group merging without deserialization using {@link ParquetFileWriter#appendFile}
 *   <li>Strict schema validation - all files must have identical {@link MessageType}
 *   <li>Metadata merging for Iceberg-specific footer data
 *   <li>Works with any FileIO implementation (local, S3, GCS, Azure, etc.)
 * </ul>
 *
 * <p>Restrictions:
 *
 * <ul>
 *   <li>All files must have compatible schemas (identical {@link MessageType})
 *   <li>Files must not be encrypted
 *   <li>Files must not have associated delete files or delete vectors
 *   <li>Table must not have a sort order (including z-ordered tables)
 * </ul>
 *
 * <p>Typical usage:
 *
 * <pre>
 * FileIO fileIO = table.io();
 * List&lt;InputFile&gt; inputFiles = Arrays.asList(
 *     fileIO.newInputFile("s3://bucket/file1.parquet"),
 *     fileIO.newInputFile("s3://bucket/file2.parquet")
 * );
 * OutputFile outputFile = fileIO.newOutputFile("s3://bucket/merged.parquet");
 * long rowGroupSize = 128 * 1024 * 1024; // 128 MB
 * int columnIndexTruncateLength = 64; // Default truncation length
 * ParquetFileMerger.mergeFiles(inputFiles, outputFile, rowGroupSize, columnIndexTruncateLength, null);
 * </pre>
 */
public class ParquetFileMerger {
  // Default buffer sizes for DeltaBinaryPackingValuesWriter
  private static final int DEFAULT_INITIAL_BUFFER_SIZE = 64 * 1024; // 64KB
  private static final int DEFAULT_PAGE_SIZE_FOR_ENCODING = 64 * 1024; // 64KB

  private ParquetFileMerger() {
    // Utility class - prevent instantiation
  }

  /**
   * Reads and validates that all input files have identical Parquet schemas.
   *
   * <p>This method works with any Iceberg FileIO implementation (S3FileIO, GCSFileIO, etc.).
   *
   * @param inputFiles List of Iceberg input files to validate
   * @return the common Parquet schema if all files have identical schemas, null otherwise
   */
  public static MessageType readAndValidateSchema(List<InputFile> inputFiles) {
    try {
      if (inputFiles == null || inputFiles.isEmpty()) {
        return null;
      }

      // Read schema from the first file
      MessageType firstSchema = readSchema(inputFiles.get(0));

      // Validate all remaining files have the same schema
      for (int i = 1; i < inputFiles.size(); i++) {
        MessageType currentSchema = readSchema(inputFiles.get(i));

        if (!firstSchema.equals(currentSchema)) {
          return null;
        }
      }

      return firstSchema;
    } catch (IllegalArgumentException | IOException e) {
      return null;
    }
  }

  /**
   * Reads the Parquet schema from an Iceberg InputFile.
   *
   * @param inputFile Iceberg input file to read schema from
   * @return MessageType schema of the Parquet file
   * @throws IOException if reading fails
   */
  private static MessageType readSchema(InputFile inputFile) throws IOException {
    return ParquetFileReader.open(ParquetIO.file(inputFile))
        .getFooter()
        .getFileMetaData()
        .getSchema();
  }

  /** Internal method to merge files when schema is already known. */
  private static void mergeFilesWithSchema(
      List<InputFile> inputFiles,
      OutputFile outputFile,
      MessageType schema,
      long rowGroupSize,
      int columnIndexTruncateLength,
      Map<String, String> extraMetadata)
      throws IOException {
    try (ParquetFileWriter writer =
        new ParquetFileWriter(
            ParquetIO.file(outputFile),
            schema,
            ParquetFileWriter.Mode.CREATE,
            rowGroupSize,
            0, // maxPaddingSize - hardcoded to 0 (same as ParquetWriter)
            columnIndexTruncateLength,
            ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH,
            ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED)) {

      writer.start();
      for (InputFile inputFile : inputFiles) {
        writer.appendFile(ParquetIO.file(inputFile));
      }

      if (extraMetadata != null && !extraMetadata.isEmpty()) {
        writer.end(extraMetadata);
      } else {
        writer.end(emptyMap());
      }
    }
  }

  /** Internal method to merge files with row IDs when base schema is already known. */
  private static void mergeFilesWithRowIdsAndSchema(
      List<InputFile> inputFiles,
      OutputFile outputFile,
      List<Long> firstRowIds,
      MessageType baseSchema,
      long rowGroupSize,
      int columnIndexTruncateLength,
      Map<String, String> extraMetadata)
      throws IOException {
    // Extend schema to include _row_id column
    MessageType extendedSchema = addRowIdColumn(baseSchema);

    // Create output writer with extended schema
    try (ParquetFileWriter writer =
        new ParquetFileWriter(
            ParquetIO.file(outputFile),
            extendedSchema,
            ParquetFileWriter.Mode.CREATE,
            rowGroupSize,
            0, // maxPaddingSize - hardcoded to 0 (same as ParquetWriter)
            columnIndexTruncateLength,
            ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH,
            ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED)) {

      writer.start();

      // Get _row_id column descriptor from extended schema
      ColumnDescriptor rowIdDescriptor =
          extendedSchema.getColumnDescription(new String[] {MetadataColumns.ROW_ID.name()});

      // Process each input file
      for (int fileIdx = 0; fileIdx < inputFiles.size(); fileIdx++) {
        InputFile inputFile = inputFiles.get(fileIdx);
        long currentRowId = firstRowIds.get(fileIdx);

        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
          List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();

          for (BlockMetaData rowGroup : rowGroups) {
            long rowCount = rowGroup.getRowCount();
            writer.startBlock(rowCount);

            // Copy all existing column chunks (binary copy)
            copyColumnChunks(writer, baseSchema, inputFile, rowGroup);

            // Write new _row_id column chunk
            writeRowIdColumnChunk(writer, rowIdDescriptor, currentRowId, rowCount);
            currentRowId += rowCount;
            writer.endBlock();
          }
        }
      }

      if (extraMetadata != null && !extraMetadata.isEmpty()) {
        writer.end(extraMetadata);
      } else {
        writer.end(emptyMap());
      }
    }
  }

  /**
   * Merges multiple Parquet files with optional row lineage preservation.
   *
   * <p>This method intelligently handles row lineage based on the input files and firstRowIds:
   *
   * <ul>
   *   <li>If firstRowIds is null/empty: performs simple binary copy merge
   *   <li>If files already have physical _row_id column: performs simple binary copy merge
   *   <li>Otherwise: synthesizes physical _row_id column from virtual metadata
   * </ul>
   *
   * @param inputFiles List of Iceberg input files to merge
   * @param encryptedOutputFile Encrypted output file for the merged result (encryption handled
   *     based on table configuration)
   * @param schema Parquet schema from the input files (assumed already validated)
   * @param firstRowIds Optional list of starting row IDs for each input file (null if no lineage
   *     needed)
   * @param rowGroupSize Target row group size in bytes
   * @param columnIndexTruncateLength Maximum length for min/max values in column index
   * @param extraMetadata Additional metadata to include in the output file footer (can be null)
   * @return true if output file has physical _row_id column, false otherwise
   * @throws IOException if I/O error occurs during merge operation
   */
  public static boolean mergeFilesWithOptionalRowIds(
      List<InputFile> inputFiles,
      EncryptedOutputFile encryptedOutputFile,
      MessageType schema,
      List<Long> firstRowIds,
      long rowGroupSize,
      int columnIndexTruncateLength,
      Map<String, String> extraMetadata)
      throws IOException {
    // Get the encrypting output file (encryption applied based on table configuration)
    OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();

    // Check if row lineage processing is requested
    boolean needsRowLineageProcessing = firstRowIds != null && !firstRowIds.isEmpty();

    if (needsRowLineageProcessing) {
      if (schema.containsField(MetadataColumns.ROW_ID.name())) {
        // Files already have physical _row_id - use simple binary copy (fastest!)
        mergeFilesWithSchema(
            inputFiles, outputFile, schema, rowGroupSize, columnIndexTruncateLength, extraMetadata);
        return true; // Output has physical _row_id from input
      } else {
        // Files have virtual _row_id - synthesize physical column
        mergeFilesWithRowIdsAndSchema(
            inputFiles,
            outputFile,
            firstRowIds,
            schema,
            rowGroupSize,
            columnIndexTruncateLength,
            extraMetadata);
        return true; // We just wrote physical _row_id
      }
    } else {
      // No row lineage processing needed - simple binary merge
      mergeFilesWithSchema(
          inputFiles, outputFile, schema, rowGroupSize, columnIndexTruncateLength, extraMetadata);
      return false; // No physical _row_id
    }
  }

  /**
   * Extends a Parquet schema by adding the _row_id metadata column.
   *
   * @param baseSchema Original Parquet schema
   * @return Extended schema with _row_id column added
   */
  private static MessageType addRowIdColumn(MessageType baseSchema) {
    // Create _row_id column: required int64 with Iceberg field ID for proper column mapping
    PrimitiveType rowIdType =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .id(MetadataColumns.ROW_ID.fieldId())
            .named(MetadataColumns.ROW_ID.name());

    // Add to existing fields
    List<Type> fields = Lists.newArrayList(baseSchema.getFields());
    fields.add(rowIdType);

    return new MessageType(baseSchema.getName(), fields);
  }

  /**
   * Writes a _row_id column chunk with sequential row IDs.
   *
   * <p>Uses DELTA_BINARY_PACKED encoding with ZSTD compression. For sequential data like row IDs,
   * delta encoding is optimal as it encodes the deltas (which are all 1) rather than the absolute
   * values, achieving excellent compression ratios.
   *
   * @param writer ParquetFileWriter to write to
   * @param rowIdDescriptor Column descriptor for _row_id
   * @param startRowId Starting row ID for this row group
   * @param rowCount Number of rows in this row group
   * @throws IOException if writing fails
   */
  private static void writeRowIdColumnChunk(
      ParquetFileWriter writer, ColumnDescriptor rowIdDescriptor, long startRowId, long rowCount)
      throws IOException {

    // Start the column chunk with ZSTD compression for maximum compression of sequential data
    writer.startColumn(rowIdDescriptor, rowCount, CompressionCodecName.ZSTD);

    // Use DELTA_BINARY_PACKED encoding for sequential row IDs
    // This encodes deltas (all 1s) instead of absolute values, achieving excellent compression
    ValuesWriter valuesWriter =
        new DeltaBinaryPackingValuesWriterForLong(
            DEFAULT_INITIAL_BUFFER_SIZE,
            DEFAULT_PAGE_SIZE_FOR_ENCODING,
            HeapByteBufferAllocator.getInstance());

    // Write sequential row IDs
    for (long i = 0; i < rowCount; i++) {
      valuesWriter.writeLong(startRowId + i);
    }

    // Get the encoded bytes
    BytesInput encodedData = valuesWriter.getBytes();
    int uncompressedSize = (int) encodedData.size();

    // Create statistics for the column
    LongStatistics stats = new LongStatistics();
    stats.setMinMax(startRowId, startRowId + rowCount - 1);
    stats.setNumNulls(0);

    // Write data page using DELTA_BINARY_PACKED encoding
    // For required column (no nulls), we don't need repetition/definition level encoding
    writer.writeDataPage(
        (int) rowCount, // valueCount
        uncompressedSize, // uncompressedSize
        encodedData, // bytes
        stats, // statistics
        Encoding.BIT_PACKED, // rlEncoding (not used for required)
        Encoding.BIT_PACKED, // dlEncoding (not used for required)
        Encoding.DELTA_BINARY_PACKED); // valuesEncoding

    // End the column chunk
    writer.endColumn();
  }

  /**
   * Copies all column chunks from a row group using binary copy.
   *
   * @param writer ParquetFileWriter to write to
   * @param baseSchema Base schema without _row_id column
   * @param inputFile Input file to read from
   * @param rowGroup Row group metadata
   * @throws IOException if copying fails
   */
  private static void copyColumnChunks(
      ParquetFileWriter writer, MessageType baseSchema, InputFile inputFile, BlockMetaData rowGroup)
      throws IOException {
    try (SeekableInputStream icebergStream = inputFile.newStream()) {
      org.apache.parquet.io.SeekableInputStream parquetStream =
          new DelegatingSeekableInputStream(icebergStream) {
            @Override
            public long getPos() throws IOException {
              return icebergStream.getPos();
            }

            @Override
            public void seek(long newPos) throws IOException {
              icebergStream.seek(newPos);
            }
          };

      for (ColumnChunkMetaData columnChunk : rowGroup.getColumns()) {
        ColumnDescriptor columnDescriptor =
            baseSchema.getColumnDescription(columnChunk.getPath().toArray());
        writer.appendColumnChunk(columnDescriptor, parquetStream, columnChunk, null, null, null);
      }
    }
  }
}
