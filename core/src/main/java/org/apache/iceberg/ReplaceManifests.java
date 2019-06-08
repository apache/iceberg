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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;


public class ReplaceManifests extends SnapshotProducer<RewriteManifests> implements RewriteManifests {
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final long manifestTargetSizeBytes;

  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final List<ManifestFile> existingManifests = Collections.synchronizedList(new ArrayList<>());
  private final List<ManifestFile> newManifests = Collections.synchronizedList(new ArrayList<>());
  private final Set<ManifestFile> processedManifests = Collections.synchronizedSet(new HashSet<>());
  private final Map<Object, WriterWrapper> writers = Collections.synchronizedMap(new HashMap<>());
  private final AtomicInteger manifestCount = new AtomicInteger(0);

  private Function<DataFile, Object> clusterByFunc;
  private Predicate<ManifestFile> predicate;

  ReplaceManifests(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.spec = ops.current().spec();
    this.manifestTargetSizeBytes =
      ops.current().propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
  }

  @Override
  protected String operation() {
    return DataOperations.REPLACE;
  }

  @Override
  public RewriteManifests set(String property, String value) {
    summaryBuilder.set(property, value);
    return this;
  }

  @Override
  protected Map<String, String> summary() {
    return summaryBuilder.build();
  }

  @Override
  public ReplaceManifests clusterBy(Function<DataFile, Object> func) {
    this.clusterByFunc = func;
    return this;
  }

  @Override
  public ReplaceManifests rewriteIf(Predicate<ManifestFile> pred) {
    this.predicate = pred;
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    Preconditions.checkNotNull(clusterByFunc, "clusterBy function cannot be null");

    List<ManifestFile> currentManifests = base.currentSnapshot().manifests();

    if (requiresRewrite(currentManifests)) {
      // run the rewrite process
      performRewrite(currentManifests);
    } else {
      // just keep any new manifests that were added since the last apply(), don't rerun
      addExistingFromNewCommit(currentManifests);
    }

    // put new manifests at the beginning
    List<ManifestFile> apply = new ArrayList<>();
    apply.addAll(newManifests);
    apply.addAll(existingManifests);

    return apply;
  }

  private boolean requiresRewrite(List<ManifestFile> currentManifests) {
    if (processedManifests.size() == 0) {
      // nothing yet processed so perform a full rewrite
      return true;
    }
    // if any processed manifest is not in the current manifest list, perform a full rewrite
    Set<ManifestFile> set = Sets.newHashSet(currentManifests);
    return processedManifests.stream().anyMatch(manifest -> !set.contains(manifest));
  }

  private void addExistingFromNewCommit(List<ManifestFile> currentManifests) {
    // keep any existing manifests as-is that were not processed
    existingManifests.clear();
    currentManifests.stream()
      .filter(manifest -> !processedManifests.contains(manifest))
      .forEach(manifest -> existingManifests.add(manifest));
  }

  private void reset() {
    cleanAll();
    existingManifests.clear();
    processedManifests.clear();
    newManifests.clear();
    writers.clear();
    summaryBuilder.clear();
  }

  private void performRewrite(List<ManifestFile> currentManifests) {
    reset();

    try {
      Tasks.foreach(currentManifests)
          .executeWith(ThreadPools.getWorkerPool())
          .run(manifest -> {
            if (predicate != null && !predicate.test(manifest)) {
              existingManifests.add(manifest);
            } else {
              processedManifests.add(manifest);
              long entryNum = manifest.addedFilesCount() + manifest.existingFilesCount() + manifest.deletedFilesCount();
              long avgEntryLen = manifest.length() / entryNum;

              try (ManifestReader reader =
                     ManifestReader.read(ops.io().newInputFile(manifest.path()), ops.current()::spec)) {
                FilteredManifest filteredManifest = reader.select(Arrays.asList("*"));
                filteredManifest.liveEntries().forEach(
                    entry -> appendEntry(entry, avgEntryLen, clusterByFunc.apply(entry.file()))
                );

              } catch (IOException x) {
                throw new RuntimeIOException(x);
              }
            }
          });
    } finally {
      Tasks.foreach(writers.values()).executeWith(ThreadPools.getWorkerPool()).run(writer -> writer.close());
    }
  }

  private void appendEntry(ManifestEntry entry, long avgEntryLen, Object key) {
    Preconditions.checkNotNull(entry, "Manifest entry cannot be null");
    Preconditions.checkNotNull(key, "Key cannot be null");

    WriterWrapper writer = getWriter(key);
    writer.addEntry(entry, avgEntryLen);
  }

  private WriterWrapper getWriter(Object key) {
    WriterWrapper writer = writers.get(key);
    if (writer == null) {
      synchronized (writers) {
        writer = writers.get(key); // check again after getting lock
        if (writer == null) {
          writer = new WriterWrapper();
          writers.put(key, writer);
        }
      }
    }
    return writer;
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    for (ManifestFile manifest : newManifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }

  long getManifestTargetSizeBytes() {
    return manifestTargetSizeBytes;
  }

  class WriterWrapper {
    private ManifestWriter writer;
    private long estimatedSize;

    synchronized void addEntry(ManifestEntry entry, long len) {
      if (writer == null) {
        writer = newWriter();
      } else if (estimatedSize >= getManifestTargetSizeBytes()) {
        close();
        writer = newWriter();
      }

      writer.addExisting(entry);
      estimatedSize += len;
    }

    private ManifestWriter newWriter() {
      estimatedSize = 0;
      return new ManifestWriter(spec, manifestPath(manifestCount.getAndIncrement()), snapshotId());
    }

    synchronized void close() {
      if (writer != null) {
        try {
          writer.close();
          newManifests.add(writer.toManifestFile());
        } catch (IOException x) {
          throw new RuntimeIOException(x);
        }
      }
    }

  }

}
