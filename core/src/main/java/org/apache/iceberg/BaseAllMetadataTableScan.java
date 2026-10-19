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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ParallelIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseAllMetadataTableScan extends BaseMetadataTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAllMetadataTableScan.class);

  BaseAllMetadataTableScan(Table table, Schema schema, MetadataTableType tableType) {
    super(table, schema, tableType);
  }

  BaseAllMetadataTableScan(
      Table table, Schema schema, MetadataTableType tableType, TableScanContext context) {
    super(table, schema, tableType, context);
  }

  @Override
  public TableScan useSnapshot(long scanSnapshotId) {
    throw new UnsupportedOperationException("Cannot select snapshot in table: " + tableType());
  }

  @Override
  public TableScan useRef(String ref) {
    throw new UnsupportedOperationException("Cannot select ref in table: " + tableType());
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException("Cannot select snapshot in table: " + tableType());
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    String metadataTableName = table().name() + "." + tableType().name().toLowerCase(Locale.ROOT);
    LOG.info(
        "Scanning metadata table {} with filter {}.",
        metadataTableName,
        ExpressionUtil.toSanitizedString(filter()));
    Listeners.notifyAll(new ScanEvent(metadataTableName, 0L, filter(), schema()));

    return doPlanFiles();
  }

  protected CloseableIterable<ManifestFile> reachableManifests(
      Function<Snapshot, Iterable<ManifestFile>> toManifests) {
    Iterable<Snapshot> snapshots = table().snapshots();
    Iterable<Iterable<ManifestFile>> manifestIterables =
        Iterables.transform(snapshots, toManifests);

    try (CloseableIterable<ManifestFile> iterable =
        new ParallelIterable<>(manifestIterables, planExecutor())) {
      return CloseableIterable.withNoopClose(Sets.newHashSet(iterable));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close parallel iterable", e);
    }
  }

  /**
   * Returns each manifest reachable from the table's snapshots, mapped to the sorted IDs of the
   * snapshots that reference it.
   */
  protected Map<ManifestFile, List<Long>> reachableManifestsWithReferenceSnapshots(
      Function<Snapshot, Iterable<ManifestFile>> toManifests) {
    Iterable<Iterable<Pair<ManifestFile, Long>>> manifestIterables =
        Iterables.transform(
            table().snapshots(),
            snapshot ->
                Iterables.transform(
                    toManifests.apply(snapshot),
                    manifest -> Pair.of(manifest, snapshot.snapshotId())));

    Map<ManifestFile, List<Long>> referenceSnapshotIds = Maps.newHashMap();
    try (CloseableIterable<Pair<ManifestFile, Long>> iterable =
        new ParallelIterable<>(manifestIterables, planExecutor())) {
      for (Pair<ManifestFile, Long> pair : iterable) {
        referenceSnapshotIds
            .computeIfAbsent(pair.first(), manifest -> Lists.newArrayList())
            .add(pair.second());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close parallel iterable", e);
    }

    referenceSnapshotIds.values().forEach(Collections::sort);
    return referenceSnapshotIds;
  }
}
