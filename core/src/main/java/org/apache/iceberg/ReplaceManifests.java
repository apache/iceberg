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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;


public class ReplaceManifests extends SnapshotProducer<RewriteManifests> implements RewriteManifests {
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final List<ManifestFile> newManifests = Lists.newArrayList();
  private final Map<Object, ManifestWriter> writers = Collections.synchronizedMap(new HashMap<>());
  private final AtomicInteger manifestCount = new AtomicInteger(0);

  private Function<DataFile, Object> clusterByFunc;
  private Function<ManifestFile, Boolean> filterFunc;

  ReplaceManifests(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.spec = ops.current().spec();
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
  public ReplaceManifests filter(Function<ManifestFile, Boolean> func) {
    this.filterFunc = func;
    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    Preconditions.checkNotNull(clusterByFunc, "clusterBy function cannot be null");

    List<ManifestFile> apply = Collections.synchronizedList(new ArrayList<>());

    try {
      Tasks.foreach(base.currentSnapshot().manifests())
          .executeWith(ThreadPools.getWorkerPool())
          .run(manifest -> {
            if (filterFunc != null && !filterFunc.apply(manifest)) {
              apply.add(manifest);
            } else {
              try (ManifestReader reader =
                     ManifestReader.read(ops.io().newInputFile(manifest.path()))) {

                FilteredManifest filteredManifest = reader.select(Lists.newArrayList("*"));
                filteredManifest.iterator().forEachRemaining(
                    file -> appendFile(file, clusterByFunc.apply(file))
                );

              } catch (IOException x) {
                throw new RuntimeIOException(x);
              }
            }
          });
    } finally {
      Tasks.foreach(writers.values()).executeWith(ThreadPools.getWorkerPool()).run(
          writer -> {
            try {
              writer.close();
            } catch (IOException x) {
              throw new RuntimeIOException(x);
            }
          }
      );
    }

    newManifests.addAll(
        writers.values()
          .stream()
          .map(ManifestWriter::toManifestFile)
          .collect(Collectors.toList())
    );


    apply.addAll(newManifests);

    return apply;
  }

  private void appendFile(DataFile file, Object key) {
    Preconditions.checkNotNull(file, "Data file cannot be null");
    Preconditions.checkNotNull(key, "Key cannot be null");

    synchronized (summaryBuilder) {
      summaryBuilder.addedFile(spec, file);
    }

    ManifestWriter writer = getWriter(key);
    synchronized (writer) {
      writer.add(file);
    }
  }

  private ManifestWriter getWriter(Object key) {
    ManifestWriter writer = writers.get(key);
    if (writer == null) {
      synchronized (writers) {
        writer = writers.get(key); // check again after getting lock
        if (writer == null) {
          writer =
            new ManifestWriter(spec, manifestPath(manifestCount.getAndIncrement()), snapshotId());
          writers.put(key, writer);
        }
      }
    }
    return writer;
  }

  @Override
  protected void cleanAll() {
    writers.values().stream().forEach(
        writer -> {
          try {
            writer.close();
          } catch (IOException x) {
            throw new RuntimeIOException(x);
          }
        }
    );

    super.cleanAll();
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    for (ManifestFile manifest : newManifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }
}
