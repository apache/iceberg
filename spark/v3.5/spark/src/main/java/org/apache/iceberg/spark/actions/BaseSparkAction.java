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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.MetadataTableType.ALL_MANIFESTS;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.iceberg.AllManifestsTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.JobGroupUtils;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseSparkAction<ThisT> {

  protected static final String MANIFEST = "Manifest";
  protected static final String MANIFEST_LIST = "Manifest List";
  protected static final String STATISTICS_FILES = "Statistics Files";
  protected static final String OTHERS = "Others";

  protected static final String FILE_PATH = "file_path";
  protected static final String LAST_MODIFIED = "last_modified";

  protected static final Splitter COMMA_SPLITTER = Splitter.on(",");
  protected static final Joiner COMMA_JOINER = Joiner.on(',');

  private static final Logger LOG = LoggerFactory.getLogger(BaseSparkAction.class);
  private static final AtomicInteger JOB_COUNTER = new AtomicInteger();
  private static final int DELETE_NUM_RETRIES = 3;
  private static final int DELETE_GROUP_SIZE = 100000;
  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final Map<String, String> options = Maps.newHashMap();

  protected BaseSparkAction(SparkSession spark) {
    this.spark = spark;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  protected SparkSession spark() {
    return spark;
  }

  protected JavaSparkContext sparkContext() {
    return sparkContext;
  }

  protected abstract ThisT self();

  public ThisT option(String name, String value) {
    options.put(name, value);
    return self();
  }

  public ThisT options(Map<String, String> newOptions) {
    options.putAll(newOptions);
    return self();
  }

  protected Map<String, String> options() {
    return options;
  }

  protected <T> T withJobGroupInfo(JobGroupInfo info, Supplier<T> supplier) {
    return JobGroupUtils.withJobGroupInfo(sparkContext, info, supplier);
  }

  protected JobGroupInfo newJobGroupInfo(String groupId, String desc) {
    return new JobGroupInfo(groupId + "-" + JOB_COUNTER.incrementAndGet(), desc);
  }

  protected Table newStaticTable(TableMetadata metadata, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadata, io);
    return new BaseTable(ops, metadata.metadataFileLocation());
  }

  protected Dataset<FileInfo> contentFileDS(Table table) {
    return contentFileDS(table, null);
  }

  protected Dataset<FileInfo> contentFileDS(Table table, Set<Long> snapshotIds) {
    Table serializableTable = SerializableTableWithSize.copyOf(table);
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(serializableTable);
    int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();

    return manifestBeanDS(table, snapshotIds, numShufflePartitions)
        .flatMap(new ReadManifest(tableBroadcast), FileInfo.ENCODER);
  }

  protected Dataset<PartitionEntryBean> partitionEntryDS(Table table) {
    Table serializableTable = SerializableTableWithSize.copyOf(table);
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(serializableTable);
    int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();

    return manifestBeanDS(table, null, numShufflePartitions)
        .flatMap(new ReadManifestForPartitionStats(tableBroadcast), PartitionEntryBean.ENCODER);
  }

  private Dataset<ManifestFileBean> manifestBeanDS(
      Table table, Set<Long> snapshotIds, int numShufflePartitions) {
    Dataset<ManifestFileBean> manifestBeanDS =
        manifestDF(table, snapshotIds)
            .selectExpr(
                "content",
                "path",
                "length",
                "0 as sequenceNumber",
                "partition_spec_id as partitionSpecId",
                "added_snapshot_id as addedSnapshotId")
            .dropDuplicates("path")
            .repartition(numShufflePartitions) // avoid adaptive execution combining tasks
            .as(ManifestFileBean.ENCODER);
    return manifestBeanDS;
  }

  protected Dataset<FileInfo> manifestDS(Table table) {
    return manifestDS(table, null);
  }

  protected Dataset<FileInfo> manifestDS(Table table, Set<Long> snapshotIds) {
    return manifestDF(table, snapshotIds)
        .select(col("path"), lit(MANIFEST).as("type"))
        .as(FileInfo.ENCODER);
  }

  private Dataset<Row> manifestDF(Table table, Set<Long> snapshotIds) {
    Dataset<Row> manifestDF = loadMetadataTable(table, ALL_MANIFESTS);
    if (snapshotIds != null) {
      Column filterCond = col(AllManifestsTable.REF_SNAPSHOT_ID.name()).isInCollection(snapshotIds);
      return manifestDF.filter(filterCond);
    } else {
      return manifestDF;
    }
  }

  protected Dataset<FileInfo> manifestListDS(Table table) {
    return manifestListDS(table, null);
  }

  protected Dataset<FileInfo> manifestListDS(Table table, Set<Long> snapshotIds) {
    List<String> manifestLists = ReachableFileUtil.manifestListLocations(table, snapshotIds);
    return toFileInfoDS(manifestLists, MANIFEST_LIST);
  }

  protected Dataset<FileInfo> statisticsFileDS(Table table, Set<Long> snapshotIds) {
    Predicate<StatisticsFile> predicate;
    if (snapshotIds == null) {
      predicate = statisticsFile -> true;
    } else {
      predicate = statisticsFile -> snapshotIds.contains(statisticsFile.snapshotId());
    }

    List<String> statisticsFiles = ReachableFileUtil.statisticsFilesLocations(table, predicate);
    return toFileInfoDS(statisticsFiles, STATISTICS_FILES);
  }

  protected Dataset<FileInfo> otherMetadataFileDS(Table table) {
    return otherMetadataFileDS(table, false /* include all reachable old metadata locations */);
  }

  protected Dataset<FileInfo> allReachableOtherMetadataFileDS(Table table) {
    return otherMetadataFileDS(table, true /* include all reachable old metadata locations */);
  }

  private Dataset<FileInfo> otherMetadataFileDS(Table table, boolean recursive) {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.addAll(ReachableFileUtil.metadataFileLocations(table, recursive));
    otherMetadataFiles.add(ReachableFileUtil.versionHintLocation(table));
    otherMetadataFiles.addAll(ReachableFileUtil.statisticsFilesLocations(table));
    return toFileInfoDS(otherMetadataFiles, OTHERS);
  }

  protected Dataset<Row> loadMetadataTable(Table table, MetadataTableType type) {
    return SparkTableUtil.loadMetadataTable(spark, table, type);
  }

  private Dataset<FileInfo> toFileInfoDS(List<String> paths, String type) {
    List<FileInfo> fileInfoList = Lists.transform(paths, path -> new FileInfo(path, type));
    return spark.createDataset(fileInfoList, FileInfo.ENCODER);
  }

  /**
   * Deletes files and keeps track of how many files were removed for each file type.
   *
   * @param executorService an executor service to use for parallel deletes
   * @param deleteFunc a delete func
   * @param files an iterator of Spark rows of the structure (path: String, type: String)
   * @return stats on which files were deleted
   */
  protected DeleteSummary deleteFiles(
      ExecutorService executorService, Consumer<String> deleteFunc, Iterator<FileInfo> files) {

    DeleteSummary summary = new DeleteSummary();

    Tasks.foreach(files)
        .retry(DELETE_NUM_RETRIES)
        .stopRetryOn(NotFoundException.class)
        .suppressFailureWhenFinished()
        .executeWith(executorService)
        .onFailure(
            (fileInfo, exc) -> {
              String path = fileInfo.getPath();
              String type = fileInfo.getType();
              LOG.warn("Delete failed for {}: {}", type, path, exc);
            })
        .run(
            fileInfo -> {
              String path = fileInfo.getPath();
              String type = fileInfo.getType();
              deleteFunc.accept(path);
              summary.deletedFile(path, type);
            });

    return summary;
  }

  protected DeleteSummary deleteFiles(SupportsBulkOperations io, Iterator<FileInfo> files) {
    DeleteSummary summary = new DeleteSummary();
    Iterator<List<FileInfo>> fileGroups = Iterators.partition(files, DELETE_GROUP_SIZE);

    Tasks.foreach(fileGroups)
        .suppressFailureWhenFinished()
        .run(fileGroup -> deleteFileGroup(fileGroup, io, summary));

    return summary;
  }

  private static void deleteFileGroup(
      List<FileInfo> fileGroup, SupportsBulkOperations io, DeleteSummary summary) {

    ListMultimap<String, FileInfo> filesByType = Multimaps.index(fileGroup, FileInfo::getType);
    ListMultimap<String, String> pathsByType =
        Multimaps.transformValues(filesByType, FileInfo::getPath);

    for (Map.Entry<String, Collection<String>> entry : pathsByType.asMap().entrySet()) {
      String type = entry.getKey();
      Collection<String> paths = entry.getValue();
      int failures = 0;
      try {
        io.deleteFiles(paths);
      } catch (BulkDeletionFailureException e) {
        failures = e.numberFailedObjects();
      }
      summary.deletedFiles(type, paths.size() - failures);
    }
  }

  static class DeleteSummary {
    private final AtomicLong dataFilesCount = new AtomicLong(0L);
    private final AtomicLong positionDeleteFilesCount = new AtomicLong(0L);
    private final AtomicLong equalityDeleteFilesCount = new AtomicLong(0L);
    private final AtomicLong manifestsCount = new AtomicLong(0L);
    private final AtomicLong manifestListsCount = new AtomicLong(0L);
    private final AtomicLong statisticsFilesCount = new AtomicLong(0L);
    private final AtomicLong otherFilesCount = new AtomicLong(0L);

    public void deletedFiles(String type, int numFiles) {
      if (FileContent.DATA.name().equalsIgnoreCase(type)) {
        dataFilesCount.addAndGet(numFiles);

      } else if (FileContent.POSITION_DELETES.name().equalsIgnoreCase(type)) {
        positionDeleteFilesCount.addAndGet(numFiles);

      } else if (FileContent.EQUALITY_DELETES.name().equalsIgnoreCase(type)) {
        equalityDeleteFilesCount.addAndGet(numFiles);

      } else if (MANIFEST.equalsIgnoreCase(type)) {
        manifestsCount.addAndGet(numFiles);

      } else if (MANIFEST_LIST.equalsIgnoreCase(type)) {
        manifestListsCount.addAndGet(numFiles);

      } else if (STATISTICS_FILES.equalsIgnoreCase(type)) {
        statisticsFilesCount.addAndGet(numFiles);

      } else if (OTHERS.equalsIgnoreCase(type)) {
        otherFilesCount.addAndGet(numFiles);

      } else {
        throw new ValidationException("Illegal file type: %s", type);
      }
    }

    public void deletedFile(String path, String type) {
      if (FileContent.DATA.name().equalsIgnoreCase(type)) {
        dataFilesCount.incrementAndGet();
        LOG.trace("Deleted data file: {}", path);

      } else if (FileContent.POSITION_DELETES.name().equalsIgnoreCase(type)) {
        positionDeleteFilesCount.incrementAndGet();
        LOG.trace("Deleted positional delete file: {}", path);

      } else if (FileContent.EQUALITY_DELETES.name().equalsIgnoreCase(type)) {
        equalityDeleteFilesCount.incrementAndGet();
        LOG.trace("Deleted equality delete file: {}", path);

      } else if (MANIFEST.equalsIgnoreCase(type)) {
        manifestsCount.incrementAndGet();
        LOG.debug("Deleted manifest: {}", path);

      } else if (MANIFEST_LIST.equalsIgnoreCase(type)) {
        manifestListsCount.incrementAndGet();
        LOG.debug("Deleted manifest list: {}", path);

      } else if (STATISTICS_FILES.equalsIgnoreCase(type)) {
        statisticsFilesCount.incrementAndGet();
        LOG.debug("Deleted statistics file: {}", path);

      } else if (OTHERS.equalsIgnoreCase(type)) {
        otherFilesCount.incrementAndGet();
        LOG.debug("Deleted other metadata file: {}", path);

      } else {
        throw new ValidationException("Illegal file type: %s", type);
      }
    }

    public long dataFilesCount() {
      return dataFilesCount.get();
    }

    public long positionDeleteFilesCount() {
      return positionDeleteFilesCount.get();
    }

    public long equalityDeleteFilesCount() {
      return equalityDeleteFilesCount.get();
    }

    public long manifestsCount() {
      return manifestsCount.get();
    }

    public long manifestListsCount() {
      return manifestListsCount.get();
    }

    public long statisticsFilesCount() {
      return statisticsFilesCount.get();
    }

    public long otherFilesCount() {
      return otherFilesCount.get();
    }

    public long totalFilesCount() {
      return dataFilesCount()
          + positionDeleteFilesCount()
          + equalityDeleteFilesCount()
          + manifestsCount()
          + manifestListsCount()
          + statisticsFilesCount()
          + otherFilesCount();
    }
  }

  private static class ReadManifest implements FlatMapFunction<ManifestFileBean, FileInfo> {
    private final Broadcast<Table> table;

    ReadManifest(Broadcast<Table> table) {
      this.table = table;
    }

    @Override
    public Iterator<FileInfo> call(ManifestFileBean manifest) {
      return new ClosingIterator<>(entries(manifest));
    }

    public CloseableIterator<FileInfo> entries(ManifestFileBean manifest) {
      ManifestContent content = manifest.content();
      FileIO io = table.getValue().io();
      Map<Integer, PartitionSpec> specs = table.getValue().specs();
      List<String> proj = ImmutableList.of(DataFile.FILE_PATH.name(), DataFile.CONTENT.name());

      switch (content) {
        case DATA:
          return CloseableIterator.transform(
              ManifestFiles.read(manifest, io, specs).select(proj).iterator(),
              ReadManifest::toFileInfo);
        case DELETES:
          return CloseableIterator.transform(
              ManifestFiles.readDeleteManifest(manifest, io, specs).select(proj).iterator(),
              ReadManifest::toFileInfo);
        default:
          throw new IllegalArgumentException("Unsupported manifest content type:" + content);
      }
    }

    static FileInfo toFileInfo(ContentFile<?> file) {
      return new FileInfo(file.path().toString(), file.content().toString());
    }
  }

  private static class ReadManifestForPartitionStats
      implements FlatMapFunction<ManifestFileBean, PartitionEntryBean> {
    private final Broadcast<Table> table;

    ReadManifestForPartitionStats(Broadcast<Table> table) {
      this.table = table;
    }

    @Override
    public Iterator<PartitionEntryBean> call(ManifestFileBean manifest) {
      CloseableIterable<PartitionEntryBean> beanIterator =
          CloseableIterable.transform(
              PartitionEntry.fromManifest(table.getValue(), manifest),
              entry -> {
                ContentFile<?> file = entry.file();

                long lastUpdatedAt = 0;
                long lastUpdatedSnapshotId = 0;
                Snapshot snapshot = table.getValue().snapshot(entry.snapshotId());
                if (snapshot != null) {
                  lastUpdatedAt = snapshot.timestampMillis();
                  lastUpdatedSnapshotId = snapshot.snapshotId();
                }

                return new PartitionEntryBean(
                    file.content(),
                    file.specId(),
                    file.partition(),
                    file.recordCount(),
                    file.fileSizeInBytes(),
                    lastUpdatedSnapshotId,
                    lastUpdatedAt);
              });

      return new ClosingIterator<>(beanIterator.iterator());
    }
  }
}
