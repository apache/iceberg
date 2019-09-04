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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;


public class BaseRewriteManifests extends SnapshotProducer<RewriteManifests> implements RewriteManifests {
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final long manifestTargetSizeBytes;

  private final List<ManifestFile> keptManifests = Collections.synchronizedList(new ArrayList<>());
  private final List<ManifestFile> newManifests = Collections.synchronizedList(new ArrayList<>());
  private final Set<ManifestFile> replacedManifests = Collections.synchronizedSet(new HashSet<>());
  private final Map<Object, WriterWrapper> writers = Collections.synchronizedMap(new HashMap<>());

  private final AtomicInteger manifestSuffix = new AtomicInteger(0);
  private final AtomicLong entryCount = new AtomicLong(0);

  private final Map<String, String> summaryProps = new HashMap<>();

  private Function<DataFile, Object> clusterByFunc;
  private Predicate<ManifestFile> predicate;

  private static final String REPLACED_CNT = "manifests-replaced";
  private static final String KEPT_CNT = "manifests-kept";
  private static final String NEW_CNT = "manifests-created";
  private static final String ENTRY_CNT = "entries-processed";

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
    summaryProps.put(property, value);
    return this;
  }

  @Override
  protected Map<String, String> summary() {
    Map<String, String> result = new HashMap<>();
    result.putAll(summaryProps);
    result.put(KEPT_CNT, Integer.toString(keptManifests.size()));
    result.put(NEW_CNT, Integer.toString(newManifests.size()));
    result.put(REPLACED_CNT, Integer.toString(replacedManifests.size()));
    result.put(ENTRY_CNT, Long.toString(entryCount.get()));
    return result;
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
    apply.addAll(keptManifests);

    return apply;
  }

  private boolean requiresRewrite(List<ManifestFile> currentManifests) {
    if (replacedManifests.size() == 0) {
      // nothing yet processed so perform a full rewrite
      return true;
    }
    // if any processed manifest is not in the current manifest list, perform a full rewrite
    Set<ManifestFile> set = Sets.newHashSet(currentManifests);
    return replacedManifests.stream().anyMatch(manifest -> !set.contains(manifest));
  }

  private void addExistingFromNewCommit(List<ManifestFile> currentManifests) {
    // keep any existing manifests as-is that were not processed
    keptManifests.clear();
    currentManifests.stream()
      .filter(manifest -> !replacedManifests.contains(manifest))
      .forEach(manifest -> keptManifests.add(manifest));
  }

  private void reset() {
    cleanAll();
    entryCount.set(0);
    keptManifests.clear();
    replacedManifests.clear();
    newManifests.clear();
    writers.clear();
  }

  private void performRewrite(List<ManifestFile> currentManifests) {
    reset();

    try {
      Tasks.foreach(currentManifests)
          .executeWith(ThreadPools.getWorkerPool())
          .run(manifest -> {
            if (predicate != null && !predicate.test(manifest)) {
              keptManifests.add(manifest);
            } else {
              replacedManifests.add(manifest);
              try (ManifestReader reader =
                     ManifestReader.read(ops.io().newInputFile(manifest.path()), ops.current()::spec)) {
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
