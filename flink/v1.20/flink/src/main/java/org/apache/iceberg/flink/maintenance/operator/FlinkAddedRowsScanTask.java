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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.Collections;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Minimal {@link FileScanTask} wrapper used when we have a bare {@link DataFile} (e.g. a new
 * staging data file from {@code SnapshotChanges#addedDataFiles}) and need to feed it into the
 * reader. For data files that already come from a planned scan, we pass the native {@link
 * FileScanTask} through directly.
 *
 * <p>A plain class rather than a record to please Kryo 2.x in Flink 1.20, both of which cannot
 * build a serializer for record classes.
 */
@Internal
class FlinkAddedRowsScanTask implements FileScanTask {

  private final DataFile file;
  private final PartitionSpec spec;
  private final List<DeleteFile> deletes;

  FlinkAddedRowsScanTask(DataFile file, PartitionSpec spec, List<DeleteFile> deletes) {
    this.file = file;
    this.spec = spec;
    // Iceberg's scan APIs return Guava ImmutableList (see BaseAddedRowsScanTask#deletes), which
    // does not round-trip through Flink's Kryo fallback. Copy into a plain ArrayList so the task
    // serializes cleanly regardless of what the caller passes.
    this.deletes = Lists.newArrayList(deletes);
  }

  FlinkAddedRowsScanTask(DataFile file, PartitionSpec spec) {
    this(file, spec, Collections.emptyList());
  }

  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public List<DeleteFile> deletes() {
    return deletes;
  }

  @Override
  public long start() {
    return 0L;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return Expressions.alwaysTrue();
  }

  @Override
  public Iterable<FileScanTask> split(long targetSplitSize) {
    return Collections.singletonList(this);
  }
}
