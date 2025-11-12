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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

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
 *   <li>Zero-copy row group merging using {@link ParquetFileWriter#appendFile}
 *   <li>Strict schema validation - all files must have identical {@link MessageType}
 *   <li>Metadata merging for Iceberg-specific footer data
 *   <li>Works with any FileIO implementation (local, S3, GCS, Azure, etc.)
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
 * ParquetFileMerger.mergeFiles(inputFiles, outputFile, null);
 * </pre>
 */
public class ParquetFileMerger {

  private ParquetFileMerger() {
    // Utility class - prevent instantiation
  }

  /**
   * Merges multiple Parquet files into a single output file at the row-group level using Iceberg
   * FileIO.
   *
   * <p>This method works with any Iceberg FileIO implementation (S3FileIO, GCSFileIO, etc.), not
   * just HadoopFileIO.
   *
   * <p>All input files must have identical Parquet schemas ({@link MessageType}), otherwise an
   * exception is thrown. The merge is performed by copying row groups directly without
   * serialization/deserialization.
   *
   * @param inputFiles List of Iceberg input files to merge
   * @param outputFile Iceberg output file for the merged result
   * @param extraMetadata Additional metadata to include in the output file footer (can be null)
   * @throws IOException if I/O error occurs during merge operation
   * @throws IllegalArgumentException if no input files provided or schemas don't match
   */
  public static void mergeFiles(
      List<InputFile> inputFiles, OutputFile outputFile, Map<String, String> extraMetadata)
      throws IOException {
    Preconditions.checkArgument(
        inputFiles != null && !inputFiles.isEmpty(), "No input files provided for merging");

    // Validate and get the common schema from the first file
    org.apache.parquet.io.InputFile firstParquetFile = ParquetIO.file(inputFiles.get(0));
    MessageType schema =
        org.apache.parquet.hadoop.ParquetFileReader.open(firstParquetFile)
            .getFooter()
            .getFileMetaData()
            .getSchema();

    // Validate all files have the same schema
    for (int i = 1; i < inputFiles.size(); i++) {
      org.apache.parquet.io.InputFile parquetFile = ParquetIO.file(inputFiles.get(i));
      MessageType currentSchema =
          org.apache.parquet.hadoop.ParquetFileReader.open(parquetFile)
              .getFooter()
              .getFileMetaData()
              .getSchema();

      if (!schema.equals(currentSchema)) {
        throw new IllegalArgumentException(
            String.format(
                "Schema mismatch detected: file '%s' has schema %s but file '%s' has schema %s. "
                    + "All files must have identical Parquet schemas for row-group level merging.",
                inputFiles.get(0).location(), schema, inputFiles.get(i).location(), currentSchema));
      }
    }

    // Create the output Parquet file writer
    org.apache.parquet.io.OutputFile parquetOutputFile = ParquetIO.file(outputFile);
    try (ParquetFileWriter writer =
        new ParquetFileWriter(
            parquetOutputFile,
            schema,
            ParquetFileWriter.Mode.CREATE,
            ParquetWriter.DEFAULT_BLOCK_SIZE,
            0)) {

      writer.start();

      // Append each input file's row groups to the output
      for (InputFile inputFile : inputFiles) {
        writer.appendFile(ParquetIO.file(inputFile));
      }

      // End writing with optional metadata
      if (extraMetadata != null && !extraMetadata.isEmpty()) {
        writer.end(extraMetadata);
      } else {
        writer.end(java.util.Collections.emptyMap());
      }
    }
  }

  /**
   * Checks if a list of Iceberg InputFiles can be merged (i.e., they all have identical schemas).
   *
   * <p>This method works with any Iceberg FileIO implementation (S3FileIO, GCSFileIO, etc.).
   *
   * @param inputFiles List of Iceberg input files to check
   * @return true if all files have identical schemas and can be merged, false otherwise
   */
  public static boolean canMerge(List<InputFile> inputFiles) {
    try {
      if (inputFiles == null || inputFiles.isEmpty()) {
        return false;
      }

      // Read schema from the first file
      org.apache.parquet.io.InputFile firstParquetFile = ParquetIO.file(inputFiles.get(0));
      MessageType firstSchema =
          org.apache.parquet.hadoop.ParquetFileReader.open(firstParquetFile)
              .getFooter()
              .getFileMetaData()
              .getSchema();

      // Validate all remaining files have the same schema
      for (int i = 1; i < inputFiles.size(); i++) {
        org.apache.parquet.io.InputFile parquetFile = ParquetIO.file(inputFiles.get(i));
        MessageType currentSchema =
            org.apache.parquet.hadoop.ParquetFileReader.open(parquetFile)
                .getFooter()
                .getFileMetaData()
                .getSchema();

        if (!firstSchema.equals(currentSchema)) {
          return false;
        }
      }

      return true;
    } catch (IllegalArgumentException | IOException e) {
      return false;
    }
  }
}
