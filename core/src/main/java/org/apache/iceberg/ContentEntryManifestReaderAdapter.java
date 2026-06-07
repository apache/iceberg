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

import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;

/**
 * Adapts a {@link ContentEntryReader} to the {@link ManifestReader} API so callers can use v4
 * content_entry manifests without code changes. Only {@link #entries()} and {@link #liveEntries()}
 * are overridden; all other methods are inherited from {@link ManifestReader}.
 *
 * @param <F> either {@link DataFile} or {@link DeleteFile}
 */
class ContentEntryManifestReaderAdapter<F extends ContentFile<F>> extends ManifestReader<F> {
  private final ContentEntryReader contentEntryReader;
  private final ManifestContent manifestContent;
  private final String manifestLocation;
  private final Long firstRowId;
  private final boolean isCommitted;

  ContentEntryManifestReaderAdapter(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      ContentEntryReader contentEntryReader,
      ManifestContent manifestContent) {
    this(file, specId, specsById, contentEntryReader, manifestContent, null, true);
  }

  ContentEntryManifestReaderAdapter(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      ContentEntryReader contentEntryReader,
      ManifestContent manifestContent,
      Long firstRowId,
      boolean isCommitted) {
    super(
        file,
        specId,
        specsById,
        InheritableMetadataFactory.empty(),
        null /* firstRowId handled in iterator() */,
        manifestContent == ManifestContent.DATA ? FileType.DATA_FILES : FileType.DELETE_FILES);
    this.contentEntryReader = contentEntryReader;
    this.manifestContent = manifestContent;
    this.manifestLocation = file.location();
    this.firstRowId = firstRowId;
    this.isCommitted = isCommitted;
    addCloseable(contentEntryReader);
  }

  @Override
  CloseableIterable<ManifestEntry<F>> entries() {
    return rawEntries();
  }

  @Override
  CloseableIterable<ManifestEntry<F>> liveEntries() {
    return CloseableIterable.filter(
        rawEntries(), entry -> entry != null && entry.status() != ManifestEntry.Status.DELETED);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private CloseableIterable<ManifestEntry<F>> rawEntries() {
    if (manifestContent == ManifestContent.DATA) {
      return (CloseableIterable<ManifestEntry<F>>)
          (CloseableIterable) contentEntryReader.dataEntries();
    } else {
      return (CloseableIterable<ManifestEntry<F>>)
          (CloseableIterable) contentEntryReader.deleteEntries();
    }
  }

  @Override
  public CloseableIterator<F> iterator() {
    // Track ordinal position and set both fileOrdinal and manifestLocation on each file so that
    // pos() and manifestLocation() return the expected values, matching the Avro reader behavior.
    // Apply firstRowId assignment following the same logic as ManifestReader.idAssigner(): if a
    // manifest-level firstRowId is present, assign sequential IDs; if the manifest is committed
    // with no firstRowId, nullify per-entry firstRowIds; if uncommitted, leave them as-is.
    return CloseableIterable.transform(
            liveEntries(),
            new java.util.function.Function<ManifestEntry<F>, F>() {
              private long ordinal = 0L;
              private long nextRowId = firstRowId != null ? firstRowId : 0L;

              @Override
              public F apply(ManifestEntry<F> entry) {
                F file = entry.file();
                if (file instanceof BaseFile) {
                  BaseFile<?> baseFile = (BaseFile<?>) file;
                  baseFile.setFileOrdinal(ordinal);
                  baseFile.setManifestLocation(manifestLocation);
                  if (firstRowId != null) {
                    // manifest-level firstRowId overrides per-entry value
                    if (baseFile.firstRowId() == null
                        && entry.status() != ManifestEntry.Status.DELETED) {
                      baseFile.setFirstRowId(nextRowId);
                      nextRowId += baseFile.recordCount();
                    }
                  } else if (isCommitted) {
                    // committed manifest with no manifest-level firstRowId: nullify per-entry value
                    baseFile.setFirstRowId(null);
                  }
                  // else: uncommitted — preserve per-entry firstRowId from tracking struct
                }

                ordinal += 1;
                return file;
              }
            })
        .iterator();
  }
}
