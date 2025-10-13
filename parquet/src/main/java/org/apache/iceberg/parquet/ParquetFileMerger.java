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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
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
 * <p>TODO: Encerypted tables are not supported
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Zero-copy row group merging using {@link ParquetFileWriter#appendFile}
 *   <li>Strict schema validation - all files must have identical {@link MessageType}
 *   <li>Metadata merging for Iceberg-specific footer data
 * </ul>
 *
 * <p>Typical usage:
 *
 * <pre>
 * Configuration conf = new Configuration();
 * ParquetFileMerger merger = new ParquetFileMerger(conf);
 * List&lt;Path&gt; inputFiles = Arrays.asList(file1, file2, file3);
 * Path outputFile = new Path("/path/to/output.parquet");
 * merger.mergeFiles(inputFiles, outputFile);
 * </pre>
 */
public class ParquetFileMerger {
  private final Configuration conf;

  public ParquetFileMerger(Configuration configuration) {
    this.conf = configuration;
  }

  /**
   * Merges multiple Parquet files into a single output file at the row-group level.
   *
   * <p>All input files must have identical Parquet schemas ({@link MessageType}), otherwise an
   * exception is thrown. The merge is performed by copying row groups directly without
   * serialization/deserialization.
   *
   * @param inputFiles List of input Parquet file paths to merge
   * @param outputFile Output file path for the merged result
   * @throws IOException if I/O error occurs during merge operation
   * @throws IllegalArgumentException if no input files provided or schemas don't match
   */
  public void mergeFiles(List<Path> inputFiles, Path outputFile) throws IOException {
    mergeFiles(inputFiles, outputFile, null);
  }

  /**
   * Merges multiple Parquet files into a single output file at the row-group level with custom
   * metadata.
   *
   * <p>All input files must have identical Parquet schemas ({@link MessageType}), otherwise an
   * exception is thrown. The merge is performed by copying row groups directly without
   * serialization/deserialization.
   *
   * @param inputFiles List of input Parquet file paths to merge
   * @param outputFile Output file path for the merged result
   * @param extraMetadata Additional metadata to include in the output file footer (can be null)
   * @throws IOException if I/O error occurs during merge operation
   * @throws IllegalArgumentException if no input files provided or schemas don't match
   */
  public void mergeFiles(List<Path> inputFiles, Path outputFile, Map<String, String> extraMetadata)
      throws IOException {
    // Validate and get the common schema
    MessageType schema = validateAndGetSchema(inputFiles);

    // Create the output Parquet file writer
    try (ParquetFileWriter writer =
        new ParquetFileWriter(conf, schema, outputFile, ParquetFileWriter.Mode.CREATE)) {

      writer.start();

      // Append each input file's row groups to the output
      for (Path inputFile : inputFiles) {
        writer.appendFile(HadoopInputFile.fromPath(inputFile, conf));
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
   * Validates that all input files have identical Parquet schemas and returns the common schema.
   *
   * <p>This method reads the Parquet metadata from each file and compares their schemas. If any
   * schema differs, an {@link IllegalArgumentException} is thrown with details about the mismatch.
   *
   * @param inputFiles List of input Parquet file paths to validate
   * @return The common {@link MessageType} schema shared by all input files
   * @throws IOException if I/O error occurs while reading file metadata
   * @throws IllegalArgumentException if no input files provided or schemas don't match
   */
  private MessageType validateAndGetSchema(List<Path> inputFiles) throws IOException {
    Preconditions.checkArgument(
        inputFiles != null && !inputFiles.isEmpty(), "No input files provided for merging");

    // Read the schema from the first file
    MessageType firstSchema =
        ParquetFileReader.readFooter(conf, inputFiles.get(0), ParquetMetadataConverter.NO_FILTER)
            .getFileMetaData()
            .getSchema();

    // Validate all remaining files have the same schema
    for (int i = 1; i < inputFiles.size(); i++) {
      MessageType currentSchema =
          ParquetFileReader.readFooter(conf, inputFiles.get(i), ParquetMetadataConverter.NO_FILTER)
              .getFileMetaData()
              .getSchema();

      if (!firstSchema.equals(currentSchema)) {
        throw new IllegalArgumentException(
            String.format(
                "Schema mismatch detected: file '%s' has schema %s but file '%s' has schema %s. "
                    + "All files must have identical Parquet schemas for row-group level merging.",
                inputFiles.get(0), firstSchema, inputFiles.get(i), currentSchema));
      }
    }

    return firstSchema;
  }

  /**
   * Checks if a list of Parquet files can be merged (i.e., they all have identical schemas).
   *
   * <p>This is a non-throwing version of {@link #validateAndGetSchema(List)} that returns a boolean
   * instead of throwing an exception.
   *
   * @param inputFiles List of input Parquet file paths to check
   * @return true if all files have identical schemas and can be merged, false otherwise
   */
  public boolean canMerge(List<Path> inputFiles) {
    try {
      validateAndGetSchema(inputFiles);
      return true;
    } catch (IllegalArgumentException | IOException e) {
      return false;
    }
  }
}
