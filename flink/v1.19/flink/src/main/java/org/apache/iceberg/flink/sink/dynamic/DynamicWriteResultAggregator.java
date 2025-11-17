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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.DeltaManifests;
import org.apache.iceberg.flink.sink.DeltaManifestsSerializer;
import org.apache.iceberg.flink.sink.FlinkManifestUtil;
import org.apache.iceberg.flink.sink.ManifestOutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operator which aggregates the individual {@link WriteResult} objects to a single {@link
 * DynamicCommittable} per checkpoint (storing the serialized {@link DeltaManifests}, jobId,
 * operatorId, checkpointId)
 */
class DynamicWriteResultAggregator
    extends AbstractStreamOperator<CommittableMessage<DynamicCommittable>>
    implements OneInputStreamOperator<
        CommittableMessage<DynamicWriteResult>, CommittableMessage<DynamicCommittable>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicWriteResultAggregator.class);
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private final CatalogLoader catalogLoader;
  private final int cacheMaximumSize;
  private transient Map<WriteTarget, Collection<DynamicWriteResult>> results;
  private transient Map<String, Map<Integer, PartitionSpec>> specs;
  private transient Map<String, ManifestOutputFileFactory> outputFileFactories;
  private transient String flinkJobId;
  private transient String operatorId;
  private transient int subTaskId;
  private transient int attemptId;
  private transient Catalog catalog;

  DynamicWriteResultAggregator(CatalogLoader catalogLoader, int cacheMaximumSize) {
    this.catalogLoader = catalogLoader;
    this.cacheMaximumSize = cacheMaximumSize;
  }

  @Override
  public void open() throws Exception {
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorId = getOperatorID().toString();
    this.subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getTaskInfo().getAttemptNumber();
    this.results = Maps.newHashMap();
    this.specs = new LRUCache<>(cacheMaximumSize);
    this.outputFileFactories = new LRUCache<>(cacheMaximumSize);
    this.catalog = catalogLoader.loadCatalog();
  }

  @Override
  public void finish() throws IOException {
    prepareSnapshotPreBarrier(Long.MAX_VALUE);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws IOException {
    Collection<CommittableWithLineage<DynamicCommittable>> committables =
        Sets.newHashSetWithExpectedSize(results.size());
    int count = 0;
    for (Map.Entry<WriteTarget, Collection<DynamicWriteResult>> entries : results.entrySet()) {
      committables.add(
          new CommittableWithLineage<>(
              new DynamicCommittable(
                  entries.getKey(),
                  writeToManifest(entries.getKey(), entries.getValue(), checkpointId),
                  getContainingTask().getEnvironment().getJobID().toString(),
                  getRuntimeContext().getOperatorUniqueID(),
                  checkpointId),
              checkpointId,
              count));
      ++count;
    }

    output.collect(
        new StreamRecord<>(
            new CommittableSummary<>(subTaskId, count, checkpointId, count, count, 0)));
    committables.forEach(
        c ->
            output.collect(
                new StreamRecord<>(
                    new CommittableWithLineage<>(c.getCommittable(), checkpointId, subTaskId))));
    LOG.info("Emitted {} commit message to downstream committer operator", count);
    results.clear();
  }

  /**
   * Write all the completed data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  @VisibleForTesting
  byte[] writeToManifest(
      WriteTarget key, Collection<DynamicWriteResult> writeResults, long checkpointId)
      throws IOException {
    if (writeResults.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult.Builder builder = WriteResult.builder();
    writeResults.forEach(w -> builder.add(w.writeResult()));
    WriteResult result = builder.build();

    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result,
            () -> outputFileFactory(key.tableName()).create(checkpointId),
            spec(key.tableName(), key.specId()));

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void processElement(StreamRecord<CommittableMessage<DynamicWriteResult>> element)
      throws Exception {

    if (element.isRecord() && element.getValue() instanceof CommittableWithLineage) {
      DynamicWriteResult result =
          ((CommittableWithLineage<DynamicWriteResult>) element.getValue()).getCommittable();
      WriteTarget key = result.key();
      results.computeIfAbsent(key, unused -> Sets.newHashSet()).add(result);
    }
  }

  private ManifestOutputFileFactory outputFileFactory(String tableName) {
    return outputFileFactories.computeIfAbsent(
        tableName,
        unused -> {
          Table table = catalog.loadTable(TableIdentifier.parse(tableName));
          specs.put(tableName, table.specs());
          // Make sure to append an identifier to avoid file clashes in case the factory was to get
          // re-created during a checkpoint, i.e. due to cache eviction.
          String fileSuffix = UUID.randomUUID().toString();
          return FlinkManifestUtil.createOutputFileFactory(
              () -> table,
              table.properties(),
              flinkJobId,
              operatorId,
              subTaskId,
              attemptId,
              fileSuffix);
        });
  }

  private PartitionSpec spec(String tableName, int specId) {
    Map<Integer, PartitionSpec> knownSpecs = specs.get(tableName);
    if (knownSpecs != null) {
      PartitionSpec spec = knownSpecs.get(specId);
      if (spec != null) {
        return spec;
      }
    }

    Table table = catalog.loadTable(TableIdentifier.parse(tableName));
    return table.specs().get(specId);
  }
}
