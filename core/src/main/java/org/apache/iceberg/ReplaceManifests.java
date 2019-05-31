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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;


@ThreadSafe
public class ReplaceManifests extends SnapshotProducer<RewriteManifests> implements RewriteManifests {
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final List<ManifestFile> newManifests = Collections.synchronizedList(Lists.newArrayList());
  private final List<ManifestFile> existingManifests = Collections.synchronizedList(Lists.newArrayList());
  private final PartitionSpec spec;
  private final Map<Object, ManifestWriter> writers = Collections.synchronizedMap(new HashMap<>());
  private AtomicInteger manifestCount = new AtomicInteger(0);

  ReplaceManifests(TableOperations ops) {
    super(ops);
    this.spec = ops.current().spec();
  }

  @Override
  protected String operation() {
    return DataOperations.APPEND;
  }

  @Override
  public RewriteManifests set(String property, String value) {
    synchronized (summaryBuilder) {
      summaryBuilder.set(property, value);
    }
    return this;
  }

  @Override
  protected Map<String, String> summary() {
    synchronized (summaryBuilder) {
      return summaryBuilder.build();
    }
  }

  @Override
  public ReplaceManifests appendFile(DataFile file, Object key) {
    Preconditions.checkNotNull(file, "Data file cannot be null");
    Preconditions.checkNotNull(key, "Key cannot be null");

    synchronized (summaryBuilder) {
      summaryBuilder.addedFile(spec, file);
    }

    ManifestWriter writer = getWriter(key);
    synchronized (writer) {
      writer.add(file);
    }

    return this;
  }

  @Override
  public ReplaceManifests keepManifest(ManifestFile manifest) {
    Preconditions.checkNotNull(manifest, "Manifest cannot be null");
    Preconditions.checkNotNull(manifest.snapshotId(), "Manifest snapshot ID cannot be null");

    existingManifests.add(manifest);
    return this;
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
  public synchronized List<ManifestFile> apply(TableMetadata base) {
    List<ManifestFile> apply = new ArrayList<>();

    Tasks.foreach(writers.values()).executeWith(ThreadPools.getWorkerPool()).run(
        writer -> {
          try {
            writer.close();
          } catch (IOException x) {
            throw new RuntimeIOException(x);
          }
          newManifests.add(writer.toManifestFile());
        }
    );

    apply.addAll(newManifests);
    apply.addAll(existingManifests);

    return apply;
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
  protected synchronized void cleanUncommitted(Set<ManifestFile> committed) {
    for (ManifestFile manifest : newManifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }
}
