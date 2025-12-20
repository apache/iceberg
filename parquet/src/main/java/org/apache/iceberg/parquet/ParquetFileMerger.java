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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.LongType;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
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
 */
public class ParquetFileMerger {
  // Default buffer sizes for DeltaBinaryPackingValuesWriter
  private static final int DEFAULT_INITIAL_BUFFER_SIZE = 64 * 1024; // 64KB
  private static final int DEFAULT_PAGE_SIZE_FOR_ENCODING = 64 * 1024; // 64KB
  private static final PrimitiveType ROW_ID_TYPE =
      Types.required(PrimitiveType.PrimitiveTypeName.INT64)
          .id(MetadataColumns.ROW_ID.fieldId())
          .named(MetadataColumns.ROW_ID.name());

  private static final PrimitiveType LAST_UPDATED_SEQUENCE_NUMBER_TYPE =
      Types.required(PrimitiveType.PrimitiveTypeName.INT64)
          .id(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId())
          .named(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());

  private static final ColumnDescriptor ROW_ID_DESCRIPTOR =
      new ColumnDescriptor(new String[] {MetadataColumns.ROW_ID.name()}, ROW_ID_TYPE, 0, 0);
  private static final ColumnDescriptor LAST_UPDATED_SEQUENCE_NUMBER_DESCRIPTOR =
      new ColumnDescriptor(
          new String[] {MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()},
          LAST_UPDATED_SEQUENCE_NUMBER_TYPE,
          0,
          0);

  private ParquetFileMerger() {
    // Utility class - prevent instantiation
  }

  /**
   * Validates that DataFiles can be merged and returns the Parquet schema if validation succeeds.
   *
   * <p>This method validates:
   *
   * <ul>
   *   <li>All files must have compatible schemas (identical {@link MessageType})
   *   <li>Files must not be encrypted
   *   <li>Files must not have associated delete files or delete vectors
   *   <li>All files have the same partition spec
   *   <li>Table must not have a sort order (including z-ordered tables)
   *   <li>No files exceed the target output size (not splitting large files)
   * </ul>
   *
   * <p>This validation is useful for compaction operations in Spark, Flink, or other engines that
   * need to ensure files can be safely merged. The returned MessageType can be passed to {@link
   * #binaryMerge} to avoid re-reading the schema.
   *
   * @param dataFiles List of DataFiles to validate
   * @param fileIO FileIO to use for reading files
   * @param targetOutputSize Maximum size for output file (files larger than this cannot be merged)
   * @return MessageType schema if files can be merged, null otherwise
   */
  public static MessageType canMergeAndGetSchema(
      List<DataFile> dataFiles, FileIO fileIO, long targetOutputSize) {
    Preconditions.checkArgument(
        dataFiles != null && !dataFiles.isEmpty(), "dataFiles cannot be null or empty");

    // Single loop to check partition spec consistency, file sizes, and build InputFile list
    int firstSpecId = dataFiles.get(0).specId();
    List<InputFile> inputFiles = Lists.newArrayListWithCapacity(dataFiles.size());
    for (DataFile dataFile : dataFiles) {
      if (dataFile.specId() != firstSpecId) {
        return null;
      }

      if (dataFile.fileSizeInBytes() > targetOutputSize) {
        return null;
      }

      inputFiles.add(fileIO.newInputFile(dataFile.location()));
    }

    return canMergeAndGetSchema(inputFiles);
  }

  private static MessageType readSchema(InputFile inputFile) throws IOException {
    ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile));
    try (reader) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }

  /**
   * Checks whether all row lineage columns in the given input files are guaranteed to have non-null
   * values based on available statistics.
   *
   * @param inputFiles the files to validate
   * @return {@code true} if statistics exist for all row lineage columns and indicate that no null
   *     values are present; {@code false} otherwise
   */
  private static boolean allRowLineageColumnsNonNull(List<InputFile> inputFiles) {
    try {
      for (InputFile inputFile : inputFiles) {
        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
          List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();

          for (BlockMetaData rowGroup : rowGroups) {
            for (ColumnChunkMetaData columnChunk : rowGroup.getColumns()) {
              String columnPath = columnChunk.getPath().toDotString();

              if (columnPath.equals(MetadataColumns.ROW_ID.name())) {
                Statistics<?> stats = columnChunk.getStatistics();
                if (stats == null || stats.getNumNulls() > 0) {
                  return false;
                }
              }

              if (columnPath.equals(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())) {
                Statistics<?> stats = columnChunk.getStatistics();
                if (stats == null || stats.getNumNulls() > 0) {
                  return false;
                }
              }
            }
          }
        }
      }
      return true;
    } catch (IOException e) {
      // If we can't read the file metadata, we can't validate
      return false;
    }
  }

  private static ParquetFileWriter writer(
      OutputFile outputFile, MessageType schema, long rowGroupSize, int columnIndexTruncateLength)
      throws IOException {
    return new ParquetFileWriter(
        ParquetIO.file(outputFile),
        schema,
        ParquetFileWriter.Mode.CREATE,
        rowGroupSize,
        0, // maxPaddingSize - hardcoded to 0 (same as ParquetWriter)
        columnIndexTruncateLength,
        ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH,
        ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED);
  }

  /** Internal method to merge files when schema is already known. */
  private static void binaryMerge(
      List<InputFile> inputFiles,
      OutputFile outputFile,
      MessageType schema,
      long rowGroupSize,
      int columnIndexTruncateLength)
      throws IOException {
    try (ParquetFileWriter writer =
        writer(outputFile, schema, rowGroupSize, columnIndexTruncateLength)) {

      Map<String, String> extraMetadata = null;
      writer.start();
      for (InputFile inputFile : inputFiles) {
        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
          // Read metadata from the first file
          if (extraMetadata == null) {
            extraMetadata = reader.getFooter().getFileMetaData().getKeyValueMetaData();
          }

          reader.appendTo(writer);
        }
      }

      writer.end(extraMetadata != null ? extraMetadata : emptyMap());
    }
  }

  /**
   * Internal method for merging files when row lineage columns must be generated. Adds both {@code
   * _row_id} and {@code _last_updated_sequence_number} to the output file, and populates their
   * values using the provided {@code firstRowIds} and {@code dataSequenceNumbers}.
   */
  private static void generateRowLineageAndMerge(
      List<InputFile> inputFiles,
      OutputFile outputFile,
      List<Long> firstRowIds,
      List<Long> dataSequenceNumbers,
      MessageType baseSchema,
      long rowGroupSize,
      int columnIndexTruncateLength)
      throws IOException {
    MessageType extendedSchema = addRowLineageColumns(baseSchema);

    try (ParquetFileWriter writer =
        writer(outputFile, extendedSchema, rowGroupSize, columnIndexTruncateLength)) {

      writer.start();

      Map<String, String> extraMetadata = null;

      for (int fileIdx = 0; fileIdx < inputFiles.size(); fileIdx++) {
        InputFile inputFile = inputFiles.get(fileIdx);
        long currentRowId = firstRowIds.get(fileIdx);
        long dataSequenceNumber = dataSequenceNumbers.get(fileIdx);

        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile))) {
          // Read metadata from first file
          if (extraMetadata == null) {
            extraMetadata = reader.getFooter().getFileMetaData().getKeyValueMetaData();
          }

          List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();

          for (BlockMetaData rowGroup : rowGroups) {
            long rowCount = rowGroup.getRowCount();
            writer.startBlock(rowCount);

            // Copy all existing column chunks (binary copy)
            copyColumnChunks(writer, baseSchema, inputFile, rowGroup);

            // Extract compression codec from existing columns to use for row lineage columns
            CompressionCodecName codec = rowGroup.getColumns().get(0).getCodec();

            // Write new _row_id column chunk (DELTA_BINARY_PACKED encoded, then compressed with
            // codec) with sequential values: currentRowId, currentRowId+1, currentRowId+2, ...
            long startRowId = currentRowId;
            writeLongColumnChunk(
                writer,
                ROW_ID_DESCRIPTOR,
                rowCount,
                codec,
                startRowId,
                startRowId + rowCount - 1,
                i -> startRowId + i);
            currentRowId += rowCount;

            // Write new _last_updated_sequence_number column chunk with constant value for all
            // rows: dataSequenceNumber, dataSequenceNumber, ...
            writeLongColumnChunk(
                writer,
                LAST_UPDATED_SEQUENCE_NUMBER_DESCRIPTOR,
                rowCount,
                codec,
                dataSequenceNumber,
                dataSequenceNumber,
                i -> dataSequenceNumber);

            writer.endBlock();
          }
        }
      }

      writer.end(extraMetadata != null ? extraMetadata : emptyMap());
    }
  }

  /**
   * Merges multiple Parquet data files with optional row lineage preservation.
   *
   * <p>This method intelligently handles row lineage based on the input files and metadata:
   *
   * <ul>
   *   <li>If row lineage is not needed (null firstRowId/dataSequenceNumber): binary copy merge
   *   <li>If row lineage is needed AND already present (physical columns exist): binary copy merge
   *   <li>If row lineage is needed AND not present: synthesizes physical row lineage columns from
   *       DataFile metadata
   * </ul>
   *
   * <p>All input files must satisfy the conditions verified by {@link #canMergeAndGetSchema(List,
   * FileIO, long)}. The {@code schema} parameter should also be obtained from this method to
   * prevent redundant file reads.
   *
   * @param dataFiles List of Iceberg DataFiles to merge
   * @param fileIO FileIO to use for reading input files
   * @param outputFile Output file for the merged result
   * @param schema Parquet schema from {@link #canMergeAndGetSchema(List, FileIO, long)}
   * @param rowGroupSize Target row group size in bytes
   * @param spec PartitionSpec for the output file
   * @param partition Partition data for the output file (null for unpartitioned tables)
   * @return DataFile representing the merged output file with complete metadata
   * @throws IOException if I/O error occurs during merge operation
   */
  public static DataFile mergeFiles(
      List<DataFile> dataFiles,
      FileIO fileIO,
      OutputFile outputFile,
      MessageType schema,
      long rowGroupSize,
      PartitionSpec spec,
      StructLike partition)
      throws IOException {
    // Convert DataFiles to InputFiles and extract row lineage metadata
    List<InputFile> inputFiles = Lists.newArrayListWithCapacity(dataFiles.size());
    List<Long> firstRowIds = Lists.newArrayListWithCapacity(dataFiles.size());
    List<Long> dataSequenceNumbers = Lists.newArrayListWithCapacity(dataFiles.size());
    boolean hasRowLineage = false;

    for (DataFile dataFile : dataFiles) {
      inputFiles.add(fileIO.newInputFile(dataFile.location()));
      firstRowIds.add(dataFile.firstRowId());
      dataSequenceNumbers.add(dataFile.dataSequenceNumber());

      if (dataFile.firstRowId() != null && dataFile.dataSequenceNumber() != null) {
        hasRowLineage = true;
      }
    }

    // Initialize columnIndexTruncateLength following the same pattern as Parquet.java
    Configuration conf =
        outputFile instanceof HadoopOutputFile
            ? new Configuration(((HadoopOutputFile) outputFile).getConf())
            : new Configuration();
    int columnIndexTruncateLength =
        conf.getInt(
            ParquetOutputFormat.COLUMN_INDEX_TRUNCATE_LENGTH,
            ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);

    // Check if we need to synthesize physical row lineage columns from virtual metadata
    boolean shouldSynthesizeRowLineage =
        hasRowLineage
            && !schema.containsField(MetadataColumns.ROW_ID.name())
            && !schema.containsField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());

    if (shouldSynthesizeRowLineage) {
      // Files have virtual row lineage - synthesize physical columns
      generateRowLineageAndMerge(
          inputFiles,
          outputFile,
          firstRowIds,
          dataSequenceNumbers,
          schema,
          rowGroupSize,
          columnIndexTruncateLength);
    } else {
      // Use simple binary copy (either no row lineage, or files already have physical columns)
      binaryMerge(inputFiles, outputFile, schema, rowGroupSize, columnIndexTruncateLength);
    }

    InputFile compactedFile = fileIO.newInputFile(outputFile.location());
    Metrics metrics = ParquetUtil.fileMetrics(compactedFile, MetricsConfig.getDefault());

    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath(compactedFile.location())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(compactedFile.getLength())
            .withMetrics(metrics);

    if (partition != null) {
      builder.withPartition(partition);
    }

    // Extract firstRowId from Parquet column statistics (for V3+ tables with row lineage)
    // The min value of _row_id column becomes firstRowId
    if (metrics.lowerBounds() != null) {
      ByteBuffer rowIdLowerBound = metrics.lowerBounds().get(MetadataColumns.ROW_ID.fieldId());
      if (rowIdLowerBound != null) {
        Long firstRowId = Conversions.fromByteBuffer(LongType.get(), rowIdLowerBound);
        builder.withFirstRowId(firstRowId);
      }
    }

    return builder.build();
  }

  @VisibleForTesting
  static MessageType canMergeAndGetSchema(List<InputFile> inputFiles) {
    Preconditions.checkArgument(
        inputFiles != null && !inputFiles.isEmpty(), "inputFiles cannot be null or empty");

    try {
      MessageType firstSchema = readSchema(inputFiles.get(0));

      for (int i = 1; i < inputFiles.size(); i++) {
        MessageType currentSchema = readSchema(inputFiles.get(i));
        if (!firstSchema.equals(currentSchema)) {
          return null;
        }
      }

      boolean hasPhysicalRowLineageColumns =
          firstSchema.containsField(MetadataColumns.ROW_ID.name())
              || firstSchema.containsField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());

      if (hasPhysicalRowLineageColumns && !allRowLineageColumnsNonNull(inputFiles)) {
        return null;
      }

      return firstSchema;
    } catch (RuntimeException | IOException e) {
      // Returns null for:
      // - Non-Parquet files (IOException when reading Parquet footer)
      // - Encrypted files (ParquetCryptoRuntimeException extends RuntimeException)
      // - Any other validation failures
      return null;
    }
  }

  /**
   * Extends a Parquet schema by adding the row lineage metadata columns: _row_id,
   * _last_updated_sequence_number.
   */
  private static MessageType addRowLineageColumns(MessageType baseSchema) {
    List<Type> fields = Lists.newArrayList(baseSchema.getFields());
    fields.add(ROW_ID_TYPE);
    fields.add(LAST_UPDATED_SEQUENCE_NUMBER_TYPE);

    return new MessageType(baseSchema.getName(), fields);
  }

  /**
   * Utility helper to write a long column chunk with DELTA_BINARY_PACKED encoding. The encoded data
   * is then compressed using the specified codec. This method handles the common pattern of writing
   * {@code _row_id} and {@code _last_updated_sequence_number} columns.
   *
   * @param writer ParquetFileWriter to write to
   * @param descriptor Column descriptor for the column
   * @param rowCount Number of rows in this row group
   * @param codec Compression codec to use (should match the file's codec)
   * @param minValue Minimum value for statistics
   * @param maxValue Maximum value for statistics
   * @param valueGenerator Function that generates the value for row i (0-based index)
   * @throws IOException if writing fails
   */
  private static void writeLongColumnChunk(
      ParquetFileWriter writer,
      ColumnDescriptor descriptor,
      long rowCount,
      CompressionCodecName codec,
      long minValue,
      long maxValue,
      LongUnaryOperator valueGenerator)
      throws IOException {

    writer.startColumn(descriptor, rowCount, codec);

    int uncompressedSize;
    BytesInput compressedData;

    try (ValuesWriter valuesWriter =
        new DeltaBinaryPackingValuesWriterForLong(
            DEFAULT_INITIAL_BUFFER_SIZE,
            DEFAULT_PAGE_SIZE_FOR_ENCODING,
            HeapByteBufferAllocator.getInstance())) {

      for (long i = 0; i < rowCount; i++) {
        valuesWriter.writeLong(valueGenerator.applyAsLong(i));
      }

      BytesInput encodedData = valuesWriter.getBytes();
      uncompressedSize = (int) encodedData.size();

      if (codec != CompressionCodecName.UNCOMPRESSED) {
        CodecFactory codecFactory =
            CodecFactory.createDirectCodecFactory(
                new Configuration(), DirectByteBufferAllocator.getInstance(), 0);
        compressedData = codecFactory.getCompressor(codec).compress(encodedData);
      } else {
        compressedData = encodedData;
      }
    }

    Statistics<?> stats =
        Statistics.getBuilderForReading(descriptor.getPrimitiveType())
            .withMax(BytesUtils.longToBytes(maxValue))
            .withMin(BytesUtils.longToBytes(minValue))
            .withNumNulls(0)
            .build();

    // For required column (no nulls), we don't need repetition/definition level encoding
    writer.writeDataPage(
        (int) rowCount,
        uncompressedSize,
        compressedData,
        stats,
        rowCount,
        Encoding.RLE,
        Encoding.RLE,
        Encoding.DELTA_BINARY_PACKED);

    writer.endColumn();
  }

  /** Copies all column chunks from a row group using binary copy. */
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
