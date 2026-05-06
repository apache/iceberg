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
import java.util.Map;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.TaskResult;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.StructLikeWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes dangling delete files from the current snapshot. A delete file is dangling if its deletes
 * no longer apply to any live data files.
 *
 * <p>The following dangling delete files are removed:
 *
 * <ul>
 *   <li>Position delete files with a data sequence number less than that of any data file in the
 *       same partition
 *   <li>Equality delete files with a data sequence number less than or equal to that of any data
 *       file in the same partition
 *   <li>Delete files in partitions with no live data files
 *   <li>Deletion vectors (Puffin files) referencing non-existent data files
 * </ul>
 */
@Internal
public class RemoveDanglingDeleteFilesProcessor extends ProcessFunction<Trigger, TaskResult> {
  private static final Logger LOG =
      LoggerFactory.getLogger(RemoveDanglingDeleteFilesProcessor.class);

  private final TableLoader tableLoader;
  private transient Table table;

  public RemoveDanglingDeleteFilesProcessor(TableLoader tableLoader) {
    Preconditions.checkNotNull(tableLoader, "Table loader should not be null");
    this.tableLoader = tableLoader;
  }

  @Override
  public void open(OpenContext parameters) throws Exception {
    tableLoader.open();
    this.table = tableLoader.loadTable();
  }

  @Override
  public void processElement(Trigger trigger, Context ctx, Collector<TaskResult> out)
      throws Exception {
    try {
      table.refresh();

      if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
        LOG.info(
            "Skipping remove dangling deletes for unpartitioned table {}: "
                + "ManifestFilterManager handles this case",
            table.name());
        out.collect(
            new TaskResult(trigger.taskId(), trigger.timestamp(), true, Collections.emptyList()));
        return;
      }

      Snapshot snapshot = table.currentSnapshot();
      if (snapshot == null) {
        LOG.info("No current snapshot for table {}, nothing to do", table.name());
        out.collect(
            new TaskResult(trigger.taskId(), trigger.timestamp(), true, Collections.emptyList()));
        return;
      }

      DeleteFileSet danglingDeletes = DeleteFileSet.create();
      danglingDeletes.addAll(findDanglingDeletes(snapshot));
      danglingDeletes.addAll(findDanglingDvs(snapshot));

      if (!danglingDeletes.isEmpty()) {
        RewriteFiles rewriteFiles = table.newRewrite();
        for (DeleteFile deleteFile : danglingDeletes) {
          LOG.debug("Removing dangling delete file {}", deleteFile.location());
          rewriteFiles.deleteFile(deleteFile);
        }

        rewriteFiles.commit();
      }

      LOG.info(
          "Successfully finished removing dangling deletes for {} at {}. Removed {} delete files.",
          table.name(),
          ctx.timestamp(),
          danglingDeletes.size());
      out.collect(
          new TaskResult(trigger.taskId(), trigger.timestamp(), true, Collections.emptyList()));
    } catch (Exception e) {
      LOG.error("Failed to remove dangling deletes for {} at {}", table.name(), ctx.timestamp(), e);
      out.collect(
          new TaskResult(trigger.taskId(), trigger.timestamp(), false, Lists.newArrayList(e)));
    }
  }

  private DeleteFileSet findDanglingDeletes(Snapshot snapshot) throws Exception {
    FileIO io = table.io();
    Map<Integer, PartitionSpec> specsById = table.specs();

    Map<Integer, Map<StructLikeWrapper, Long>> minDataSeqByPartition = Maps.newHashMap();
    Map<Integer, StructLikeWrapper> wrappersBySpec = Maps.newHashMap();

    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, specsById)) {
        for (DataFile file : reader) {
          Long seq = file.dataSequenceNumber();
          if (seq == null) {
            continue;
          }

          int specId = file.specId();
          StructLikeWrapper template =
              wrappersBySpec.computeIfAbsent(
                  specId, id -> StructLikeWrapper.forType(specsById.get(id).partitionType()));

          StructLikeWrapper partitionKey = template.copyFor(file.partition());
          minDataSeqByPartition
              .computeIfAbsent(specId, id -> Maps.newHashMap())
              .merge(partitionKey, seq, Math::min);
        }
      }
    }

    DeleteFileSet danglingDeletes = DeleteFileSet.create();

    for (ManifestFile manifest : snapshot.deleteManifests(io)) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
        for (DeleteFile file : reader) {
          if (isDanglingBySequenceNumber(file, minDataSeqByPartition, wrappersBySpec, specsById)) {
            danglingDeletes.add(file);
          }
        }
      }
    }

    return danglingDeletes;
  }

  private DeleteFileSet findDanglingDvs(Snapshot snapshot) throws Exception {
    FileIO io = table.io();
    Map<Integer, PartitionSpec> specsById = table.specs();

    Set<String> dvReferencedPaths = Sets.newHashSet();
    DeleteFileSet dvCandidates = DeleteFileSet.create();

    for (ManifestFile manifest : snapshot.deleteManifests(io)) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
        for (DeleteFile file : reader) {
          if (file.format() == FileFormat.PUFFIN && file.referencedDataFile() != null) {
            dvReferencedPaths.add(file.referencedDataFile());
            dvCandidates.add(file);
          }
        }
      }
    }

    if (dvCandidates.isEmpty()) {
      return dvCandidates;
    }

    for (ManifestFile manifest : snapshot.dataManifests(io)) {
      if (dvReferencedPaths.isEmpty()) {
        break;
      }

      try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, specsById)) {
        for (DataFile file : reader) {
          dvReferencedPaths.remove(file.location());
          if (dvReferencedPaths.isEmpty()) {
            break;
          }
        }
      }
    }

    DeleteFileSet danglingDvs = DeleteFileSet.create();
    for (DeleteFile dv : dvCandidates) {
      if (dvReferencedPaths.contains(dv.referencedDataFile())) {
        danglingDvs.add(dv);
      }
    }

    return danglingDvs;
  }

  private static boolean isDanglingBySequenceNumber(
      DeleteFile file,
      Map<Integer, Map<StructLikeWrapper, Long>> minDataSeqByPartition,
      Map<Integer, StructLikeWrapper> wrappersBySpec,
      Map<Integer, PartitionSpec> specsById) {
    Long seq = file.dataSequenceNumber();
    if (seq == null) {
      return false;
    }

    int specId = file.specId();
    StructLikeWrapper template =
        wrappersBySpec.computeIfAbsent(
            specId, id -> StructLikeWrapper.forType(specsById.get(id).partitionType()));
    StructLikeWrapper partitionKey = template.copyFor(file.partition());

    Map<StructLikeWrapper, Long> partitionMap = minDataSeqByPartition.get(specId);
    Long minDataSeq = partitionMap != null ? partitionMap.get(partitionKey) : null;

    if (minDataSeq == null) {
      return true;
    }

    if (file.content() == FileContent.POSITION_DELETES && seq < minDataSeq) {
      return true;
    }

    return file.content() == FileContent.EQUALITY_DELETES && seq <= minDataSeq;
  }

  @Override
  public void close() throws Exception {
    super.close();
    tableLoader.close();
  }
}
