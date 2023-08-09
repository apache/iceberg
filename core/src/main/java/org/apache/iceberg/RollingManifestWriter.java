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
package org.apache.iceberg;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** As opposed to {@link ManifestWriter}, a rolling writer could produce multiple manifest files. */
public class RollingManifestWriter<F extends ContentFile<F>> implements Closeable {
  private static final int ROWS_DIVISOR = 250;

  private final Supplier<ManifestWriter<F>> manifestWriterSupplier;
  private final long targetFileSizeInBytes;
  private final List<ManifestFile> manifestFiles;

  private long currentFileRows = 0;
  private ManifestWriter<F> currentWriter = null;

  private boolean closed = false;

  public RollingManifestWriter(
      Supplier<ManifestWriter<F>> manifestWriterSupplier, long targetFileSizeInBytes) {
    this.manifestWriterSupplier = manifestWriterSupplier;
    this.targetFileSizeInBytes = targetFileSizeInBytes;
    this.manifestFiles = Lists.newArrayList();
  }

  /**
   * Add an added entry for a file.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. The data and file sequence
   * numbers will be assigned at commit.
   *
   * @param addedFile a data file
   */
  public void add(F addedFile) {
    currentWriter().add(addedFile);
    currentFileRows++;
  }

  /**
   * Add an added entry for a file with a specific sequence number.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. The entry's data sequence
   * number will be the provided data sequence number. The entry's file sequence number will be
   * assigned at commit.
   *
   * @param addedFile a data file
   * @param dataSequenceNumber a data sequence number for the file
   */
  public void add(F addedFile, long dataSequenceNumber) {
    currentWriter().add(addedFile, dataSequenceNumber);
    currentFileRows++;
  }

  /**
   * Add an existing entry for a file.
   *
   * <p>The original data and file sequence numbers, snapshot ID, which were assigned at commit,
   * must be preserved when adding an existing entry.
   *
   * @param existingFile a file
   * @param fileSnapshotId snapshot ID when the data file was added to the table
   * @param dataSequenceNumber a data sequence number of the file (assigned when the file was added)
   * @param fileSequenceNumber a file sequence number (assigned when the file was added)
   */
  public void existing(
      F existingFile, long fileSnapshotId, long dataSequenceNumber, Long fileSequenceNumber) {
    currentWriter().existing(existingFile, fileSnapshotId, dataSequenceNumber, fileSequenceNumber);
    currentFileRows++;
  }

  /**
   * Add a delete entry for a file.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. However, the original data and
   * file sequence numbers of the file must be preserved when the file is marked as deleted.
   *
   * @param deletedFile a file
   * @param dataSequenceNumber a data sequence number of the file (assigned when the file was added)
   * @param fileSequenceNumber a file sequence number (assigned when the file was added)
   */
  public void delete(F deletedFile, long dataSequenceNumber, Long fileSequenceNumber) {
    currentWriter().delete(deletedFile, dataSequenceNumber, fileSequenceNumber);
    currentFileRows++;
  }

  private ManifestWriter<F> currentWriter() {
    if (currentWriter == null) {
      this.currentWriter = manifestWriterSupplier.get();
    } else if (shouldRollToNewFile()) {
      closeCurrentWriter();
      this.currentWriter = manifestWriterSupplier.get();
    }

    return currentWriter;
  }

  private boolean shouldRollToNewFile() {
    return currentFileRows % ROWS_DIVISOR == 0 && currentWriter.length() >= targetFileSizeInBytes;
  }

  private void closeCurrentWriter() {
    if (currentWriter != null) {
      try {
        currentWriter.close();
        ManifestFile currentFile = currentWriter.toManifestFile();
        manifestFiles.add(currentFile);
        this.currentWriter = null;
        this.currentFileRows = 0;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close current writer", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrentWriter();
      this.closed = true;
    }
  }

  public List<ManifestFile> toManifestFiles() {
    Preconditions.checkState(closed, "Cannot get ManifestFile list from unclosed writer");
    return manifestFiles;
  }
}
