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
package org.apache.iceberg.index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reads and writes {@link IndexMetadata} JSON files using Iceberg's {@link FileIO}.
 *
 * <p>Metadata file naming follows the pattern:
 * {@code {index_location}/metadata/{version:05d}-{uuid}.metadata.json}
 *
 * <p>Example: {@code s3://warehouse/db/orders/index/order_id_idx/metadata/00001-abc123.metadata.json}
 */
public class IndexMetadataIO {

  private IndexMetadataIO() {}

  /**
   * Read index metadata from a file path.
   *
   * @param io the FileIO to use for reading
   * @param metadataLocation the full path to the metadata JSON file
   * @return the parsed IndexMetadata with metadataFileLocation set
   */
  public static IndexMetadata read(FileIO io, String metadataLocation) {
    Preconditions.checkNotNull(io, "FileIO is required");
    Preconditions.checkNotNull(metadataLocation, "metadataLocation is required");

    InputFile inputFile = io.newInputFile(metadataLocation);
    try (InputStream stream = inputFile.newStream()) {
      String json = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      IndexMetadata metadata = IndexMetadataParser.fromJson(json);
      // Return with metadataFileLocation set so callers can use it for CAS
      return GenericIndexMetadata.buildFrom(metadata)
          .metadataFileLocation(metadataLocation)
          .build();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read index metadata from: " + metadataLocation, e);
    }
  }

  /**
   * Write index metadata to an OutputFile.
   *
   * @param metadata the index metadata to write
   * @param outputFile the output file to write to
   */
  public static void write(IndexMetadata metadata, OutputFile outputFile) {
    Preconditions.checkNotNull(metadata, "metadata is required");
    Preconditions.checkNotNull(outputFile, "outputFile is required");

    String json = IndexMetadataParser.toJson(metadata, true);
    try (OutputStream stream = outputFile.createOrOverwrite()) {
      stream.write(json.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to write index metadata to: " + outputFile.location(), e);
    }
  }

  /**
   * Generate the next metadata file location for an index.
   *
   * <p>The version is derived from the number of existing snapshots + 1, ensuring monotonically
   * increasing names for easy visual ordering. A UUID suffix guarantees uniqueness even if two
   * writers generate the same version number concurrently.
   *
   * @param indexLocation the base location of the index
   * @param currentMetadata the current metadata, or null if this is the first write
   * @return the new metadata file path
   */
  public static String newMetadataFileLocation(
      String indexLocation, IndexMetadata currentMetadata) {
    int version = currentMetadata == null ? 1 : currentMetadata.snapshots().size() + 1;
    String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    return String.format(
        "%s/metadata/%05d-%s.metadata.json",
        indexLocation.replaceAll("/$", ""), version, uuid);
  }
}
