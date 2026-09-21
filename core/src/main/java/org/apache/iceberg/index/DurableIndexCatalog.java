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
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * {@link IndexCatalog} implementation that persists each index's current metadata-file pointer
 * as a small file under the table's own location, so registrations survive a process restart --
 * unlike {@link InMemoryIndexCatalog}, which loses them.
 *
 * <p>The index metadata, tracking, and leaf files an index build produces are already written to
 * durable storage regardless of which {@link IndexCatalog} is used ({@link ScalarIndexCommitter}
 * writes them directly via {@link FileIO}). What is not durable without this class is purely the
 * pointer telling a fresh process where to find the current metadata file for a given index name
 * -- exactly the gap this fills. One pointer file per index name, at {@code
 * <tableLocation>/metadata/scalar-indexes/<indexName>.pointer}, containing just the current
 * metadata file's location as plain text.
 *
 * <p>Optimistic concurrency is read-then-conditional-write against that pointer file, not a
 * cross-process lock -- adequate for the same single-writer-at-a-time assumption {@link
 * InMemoryIndexCatalog} already documents, not a guarantee under concurrent index builds on the
 * same table.
 *
 * <p>{@link #listIndexes} requires the table's {@link FileIO} to implement {@link
 * SupportsPrefixOperations} (true for most durable backends, e.g. S3FileIO) to list the pointer
 * files under the registry directory; it throws {@link UnsupportedOperationException} otherwise
 * rather than guessing.
 */
public class DurableIndexCatalog implements IndexCatalog {

  private final FileIO io;
  private final String registryLocation;

  public DurableIndexCatalog(FileIO io, String tableLocation) {
    Preconditions.checkNotNull(io, "FileIO is required");
    Preconditions.checkNotNull(tableLocation, "tableLocation is required");
    this.io = io;
    this.registryLocation =
        stripTrailingSlash(tableLocation) + "/metadata/scalar-indexes";
  }

  private String pointerLocation(IndexIdentifier identifier) {
    return registryLocation + "/" + identifier.name() + ".pointer";
  }

  @Override
  public void createIndex(IndexIdentifier identifier, IndexMetadata metadata) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    Preconditions.checkNotNull(metadata, "metadata is required");
    Preconditions.checkArgument(
        metadata.metadataFileLocation() != null,
        "metadata must have a metadataFileLocation set before registering");

    if (indexExists(identifier)) {
      throw new AlreadyExistsException("Index already exists: %s", identifier);
    }
    writePointer(identifier, metadata.metadataFileLocation());
  }

  @Override
  public IndexMetadata loadIndex(IndexIdentifier identifier) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    String metadataLocation = readPointerOrNull(identifier);
    if (metadataLocation == null) {
      throw new NoSuchTableException("Index does not exist: %s", identifier);
    }
    return IndexMetadataIO.read(io, metadataLocation);
  }

  @Override
  public void updateIndex(IndexIdentifier identifier, IndexMetadata base, IndexMetadata updated) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    Preconditions.checkNotNull(base, "base metadata is required");
    Preconditions.checkNotNull(updated, "updated metadata is required");
    Preconditions.checkArgument(
        updated.metadataFileLocation() != null,
        "updated metadata must have a metadataFileLocation set");

    String currentLocation = readPointerOrNull(identifier);
    if (currentLocation == null) {
      throw new NoSuchTableException("Index does not exist: %s", identifier);
    }
    if (!currentLocation.equals(base.metadataFileLocation())) {
      throw new ConcurrentModificationException(
          String.format(
              Locale.ROOT,
              "Cannot update index %s: current metadata location has changed. Expected: %s",
              identifier, base.metadataFileLocation()));
    }
    writePointer(identifier, updated.metadataFileLocation());
  }

  @Override
  public void dropIndex(IndexIdentifier identifier) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    if (!indexExists(identifier)) {
      throw new NoSuchTableException("Index does not exist: %s", identifier);
    }
    io.deleteFile(pointerLocation(identifier));
  }

  @Override
  public boolean indexExists(IndexIdentifier identifier) {
    Preconditions.checkNotNull(identifier, "identifier is required");
    return io.newInputFile(pointerLocation(identifier)).exists();
  }

  @Override
  public List<IndexMetadata> listIndexes(TableIdentifier tableIdentifier) {
    Preconditions.checkNotNull(tableIdentifier, "tableIdentifier is required");
    if (!(io instanceof SupportsPrefixOperations)) {
      throw new UnsupportedOperationException(
          "listIndexes requires a FileIO that supports prefix listing (SupportsPrefixOperations),"
              + " but got: " + io.getClass().getName());
    }

    List<IndexMetadata> indexes = Lists.newArrayList();
    for (FileInfo file : ((SupportsPrefixOperations) io).listPrefix(registryLocation + "/")) {
      String location = file.location();
      if (!location.endsWith(".pointer")) {
        continue;
      }
      String indexName =
          location.substring(
              location.lastIndexOf('/') + 1, location.length() - ".pointer".length());
      IndexIdentifier identifier = IndexIdentifier.of(tableIdentifier, indexName);
      indexes.add(loadIndex(identifier));
    }
    return indexes;
  }

  private String readPointerOrNull(IndexIdentifier identifier) {
    InputFile pointerFile = io.newInputFile(pointerLocation(identifier));
    if (!pointerFile.exists()) {
      return null;
    }
    try (InputStream in = pointerFile.newStream()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read index pointer: " + pointerFile.location(), e);
    }
  }

  private void writePointer(IndexIdentifier identifier, String metadataLocation) {
    OutputFile pointerFile = io.newOutputFile(pointerLocation(identifier));
    try (OutputStream out = pointerFile.createOrOverwrite()) {
      out.write(metadataLocation.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write index pointer: " + pointerFile.location(), e);
    }
  }

  private static String stripTrailingSlash(String path) {
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
