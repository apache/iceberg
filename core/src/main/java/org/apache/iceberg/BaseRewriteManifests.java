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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;


public class BaseRewriteManifests extends SnapshotProducer<RewriteManifests> implements RewriteManifests {
  private static final String KEPT_MANIFESTS_COUNT = "manifests-kept";
  private static final String CREATED_MANIFESTS_COUNT = "manifests-created";
  private static final String REPLACED_MANIFESTS_COUNT = "manifests-replaced";
  private static final String PROCESSED_ENTRY_COUNT = "entries-processed";

  private static final ImmutableSet<ManifestEntry.Status> ALLOWED_ENTRY_STATUSES = ImmutableSet.of(
      ManifestEntry.Status.EXISTING);

  private final TableOperations ops;
  private final PartitionSpec spec;
  private final long manifestTargetSizeBytes;

  private final Set<ManifestFile> deletedManifests = Sets.newHashSet();
  private final List<ManifestFile> addedManifests = Lists.newArrayList();

  private final List<ManifestFile> keptManifests = Collections.synchronizedList(new ArrayList<>());
  private final List<ManifestFile> newManifests = Collections.synchronizedList(new ArrayList<>());
  private final Set<ManifestFile> rewrittenManifests = Collections.synchronizedSet(new HashSet<>());
  private final Map<Object, WriterWrapper> writers = Collections.synchronizedMap(new HashMap<>());

  private final AtomicInteger manifestSuffix = new AtomicInteger(0);
  private final AtomicLong entryCount = new AtomicLong(0);

  private Function<DataFile, Object> clusterByFunc;
  private Predicate<ManifestFile> predicate;

  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

  BaseRewriteManifests(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.spec = ops.current().spec();
    this.manifestTargetSizeBytes =
      ops.current().propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
  }

  @Override
  protected RewriteManifests self() {
    return this;
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
    summaryBuilder.set(KEPT_MANIFESTS_COUNT, String.valueOf(keptManifests.size()));
    summaryBuilder.set(CREATED_MANIFESTS_COUNT, String.valueOf(newManifests.size() + addedManifests.size()));
    summaryBuilder.set(REPLACED_MANIFESTS_COUNT, String.valueOf(rewrittenManifests.size() + deletedManifests.size()));
    summaryBuilder.set(PROCESSED_ENTRY_COUNT, String.valueOf(entryCount.get()));
    return summaryBuilder.build();
  }

  @Override
  public RewriteManifests clusterBy(Function<DataFile, Object> func) {
    this.clusterByFunc = func;
    return this;
  }

  @Override
  public RewriteManifests rewriteIf(Predicate<ManifestFile> pred) {
    this.predicate = pred;
    return this;
  }

  @Override
  public RewriteManifests deleteManifest(ManifestFile manifest) {
    deletedManifests.add(manifest);
    return this;
  }

  @Override
  public RewriteManifests addManifest(ManifestFile manifest) {
    try {
      // the appended manifest must be rewritten with this update's snapshot ID
      addedManifests.add(copyManifest(manifest));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Cannot append manifest: " + e.getMessage());
    }
    return this;
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    Map<Integer, PartitionSpec> specsById = ops.current().specsById();
    try (ManifestReader reader = ManifestReader.read(ops.io().newInputFile(manifest.path()), specsById)) {
      OutputFile newFile = manifestPath(manifestSuffix.getAndIncrement());
      return ManifestWriter.copyManifest(reader, newFile, snapshotId(), summaryBuilder, ALLOWED_ENTRY_STATUSES);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", manifest);
    }
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    List<ManifestFile> currentManifests = base.currentSnapshot().manifests();
    Set<ManifestFile> currentManifestSet = ImmutableSet.copyOf(currentManifests);

    validateDeletedManifests(currentManifestSet);

    if (requiresRewrite(currentManifestSet)) {
      performRewrite(currentManifests);
    } else {
      keepActiveManifests(currentManifests);
    }

    validateFilesCounts();

    // put new manifests at the beginning
    List<ManifestFile> apply = new ArrayList<>();
    apply.addAll(newManifests);
    apply.addAll(addedManifests);
    apply.addAll(keptManifests);

    return apply;
  }

  private boolean requiresRewrite(Set<ManifestFile> currentManifests) {
    if (clusterByFunc == null) {
      // manifests are deleted and added directly so don't perform a rewrite
      return false;
    }

    if (rewrittenManifests.size() == 0) {
      // nothing yet processed so perform a full rewrite
      return true;
    }

    // if any processed manifest is not in the current manifest list, perform a full rewrite
    return rewrittenManifests.stream().anyMatch(manifest -> !currentManifests.contains(manifest));
  }

  private void keepActiveManifests(List<ManifestFile> currentManifests) {
    // keep any existing manifests as-is that were not processed
    keptManifests.clear();
    currentManifests.stream()
      .filter(manifest -> !rewrittenManifests.contains(manifest) && !deletedManifests.contains(manifest))
      .forEach(manifest -> keptManifests.add(manifest));
  }

  private void reset() {
    cleanUncommitted(newManifests, ImmutableSet.of());
    entryCount.set(0);
    keptManifests.clear();
    rewrittenManifests.clear();
    newManifests.clear();
    writers.clear();
  }

  private void performRewrite(List<ManifestFile> currentManifests) {
    reset();

    List<ManifestFile> remainingManifests = currentManifests.stream()
        .filter(manifest -> !deletedManifests.contains(manifest))
        .collect(Collectors.toList());

    try {
      Tasks.foreach(remainingManifests)
          .executeWith(ThreadPools.getWorkerPool())
          .run(manifest -> {
            if (predicate != null && !predicate.test(manifest)) {
              keptManifests.add(manifest);
            } else {
              rewrittenManifests.add(manifest);
              try (ManifestReader reader =
                     ManifestReader.read(ops.io().newInputFile(manifest.path()), ops.current().specsById())) {
                FilteredManifest filteredManifest = reader.select(Arrays.asList("*"));
                filteredManifest.liveEntries().forEach(
                    entry -> appendEntry(entry, clusterByFunc.apply(entry.file()))
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

  private void validateDeletedManifests(Set<ManifestFile> currentManifests) {
    // directly deleted manifests must be still present in the current snapshot
    deletedManifests.stream()
        .filter(manifest -> !currentManifests.contains(manifest))
        .findAny()
        .ifPresent(manifest -> {
          throw new ValidationException("Manifest is missing: %s", manifest.path());
        });
  }

  private void validateFilesCounts() {
    int createdManifestsFilesCount = activeFilesCount(newManifests) + activeFilesCount(addedManifests);
    int replacedManifestsFilesCount = activeFilesCount(rewrittenManifests) + activeFilesCount(deletedManifests);

    if (createdManifestsFilesCount != replacedManifestsFilesCount) {
      throw new ValidationException(
          "Replaced and created manifests must have the same number of active files: %d (new), %d (old)",
          createdManifestsFilesCount, replacedManifestsFilesCount);
    }
  }

  private int activeFilesCount(Iterable<ManifestFile> manifests) {
    int activeFilesCount = 0;

    for (ManifestFile manifest : manifests) {
      Preconditions.checkNotNull(manifest.addedFilesCount(), "Missing file counts in %s", manifest.path());
      Preconditions.checkNotNull(manifest.existingFilesCount(), "Missing file counts in %s", manifest.path());
      activeFilesCount += manifest.addedFilesCount();
      activeFilesCount += manifest.existingFilesCount();
    }

    return activeFilesCount;
  }

  private void appendEntry(ManifestEntry entry, Object key) {
    Preconditions.checkNotNull(entry, "Manifest entry cannot be null");
    Preconditions.checkNotNull(key, "Key cannot be null");

    WriterWrapper writer = getWriter(key);
    writer.addEntry(entry);
    entryCount.incrementAndGet();
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
    cleanUncommitted(newManifests, committed);
    cleanUncommitted(addedManifests, committed);
  }

  private void cleanUncommitted(Iterable<ManifestFile> manifests, Set<ManifestFile> committedManifests) {
    for (ManifestFile manifest : manifests) {
      if (!committedManifests.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }

  long getManifestTargetSizeBytes() {
    return manifestTargetSizeBytes;
  }

  class WriterWrapper {
    private ManifestWriter writer;

    synchronized void addEntry(ManifestEntry entry) {
      if (writer == null) {
        writer = newWriter();
      } else if (writer.length() >= getManifestTargetSizeBytes()) {
        close();
        writer = newWriter();
      }
      writer.existing(entry);
    }

    private ManifestWriter newWriter() {
      return new ManifestWriter(spec, manifestPath(manifestSuffix.getAndIncrement()), snapshotId());
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
