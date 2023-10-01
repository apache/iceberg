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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.JobGroupUtils;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.actions.ManifestFileBean;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

/**
 * A batch data scan that can utilize Spark cluster resources for planning.
 *
 * <p>This scan remotely filters manifests, fetching only the relevant data and delete files to the
 * driver. The delete file assignment is done locally after the remote filtering step. Such approach
 * is beneficial if the remote parallelism is much higher than the number of driver cores.
 *
 * <p>This scan is best suited for queries with selective filters on lower/upper bounds across all
 * partitions, or against poorly clustered metadata. This allows job planning to benefit from highly
 * concurrent remote filtering while not incurring high serialization and data transfer costs. This
 * class is also useful for full table scans over large tables but the cost of bringing data and
 * delete file details to the driver may become noticeable. Make sure to follow the performance tips
 * below in such cases.
 *
 * <p>Ensure the filtered metadata size doesn't exceed the driver's max result size. For large table
 * scans, consider increasing `spark.driver.maxResultSize` to avoid job failures.
 *
 * <p>Performance tips:
 *
 * <ul>
 *   <li>Enable Kryo serialization (`spark.serializer`)
 *   <li>Increase the number of driver cores (`spark.driver.cores`)
 *   <li>Tune the number of threads used to fetch task results (`spark.resultGetter.threads`)
 * </ul>
 */
public class SparkDistributedDataScan extends BaseDistributedDataScan {

  private static final Joiner COMMA = Joiner.on(',');
  private static final String DELETE_PLANNING_JOB_GROUP_ID = "DELETE-PLANNING";
  private static final String DATA_PLANNING_JOB_GROUP_ID = "DATA-PLANNING";

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final SparkReadConf readConf;

  private Broadcast<Table> tableBroadcast = null;

  public SparkDistributedDataScan(SparkSession spark, Table table, SparkReadConf readConf) {
    this(spark, table, readConf, table.schema(), newTableScanContext(table));
  }

  private SparkDistributedDataScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema schema,
      TableScanContext context) {
    super(table, schema, context);
    this.spark = spark;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.readConf = readConf;
  }

  @Override
  protected BatchScan newRefinedScan(
      Table newTable, Schema newSchema, TableScanContext newContext) {
    return new SparkDistributedDataScan(spark, newTable, readConf, newSchema, newContext);
  }

  @Override
  protected int remoteParallelism() {
    return readConf.parallelism();
  }

  @Override
  protected PlanningMode dataPlanningMode() {
    return readConf.dataPlanningMode();
  }

  @Override
  protected boolean shouldCopyRemotelyPlannedDataFiles() {
    return false;
  }

  @Override
  protected Iterable<CloseableIterable<DataFile>> planDataRemotely(
      List<ManifestFile> dataManifests, boolean withColumnStats) {
    JobGroupInfo info = new JobGroupInfo(DATA_PLANNING_JOB_GROUP_ID, jobDesc("data"));
    return withJobGroupInfo(info, () -> doPlanDataRemotely(dataManifests, withColumnStats));
  }

  private Iterable<CloseableIterable<DataFile>> doPlanDataRemotely(
      List<ManifestFile> dataManifests, boolean withColumnStats) {
    scanMetrics().scannedDataManifests().increment(dataManifests.size());

    JavaRDD<DataFile> dataFileRDD =
        sparkContext
            .parallelize(toBeans(dataManifests), dataManifests.size())
            .flatMap(new ReadDataManifest(tableBroadcast(), context(), withColumnStats));
    List<List<DataFile>> dataFileGroups = collectPartitions(dataFileRDD);

    int matchingFilesCount = dataFileGroups.stream().mapToInt(List::size).sum();
    int skippedFilesCount = liveFilesCount(dataManifests) - matchingFilesCount;
    scanMetrics().skippedDataFiles().increment(skippedFilesCount);

    return Iterables.transform(dataFileGroups, CloseableIterable::withNoopClose);
  }

  @Override
  protected PlanningMode deletePlanningMode() {
    return readConf.deletePlanningMode();
  }

  @Override
  protected DeleteFileIndex planDeletesRemotely(List<ManifestFile> deleteManifests) {
    JobGroupInfo info = new JobGroupInfo(DELETE_PLANNING_JOB_GROUP_ID, jobDesc("deletes"));
    return withJobGroupInfo(info, () -> doPlanDeletesRemotely(deleteManifests));
  }

  private DeleteFileIndex doPlanDeletesRemotely(List<ManifestFile> deleteManifests) {
    scanMetrics().scannedDeleteManifests().increment(deleteManifests.size());

    List<DeleteFile> deleteFiles =
        sparkContext
            .parallelize(toBeans(deleteManifests), deleteManifests.size())
            .flatMap(new ReadDeleteManifest(tableBroadcast(), context()))
            .collect();

    int skippedFilesCount = liveFilesCount(deleteManifests) - deleteFiles.size();
    scanMetrics().skippedDeleteFiles().increment(skippedFilesCount);

    return DeleteFileIndex.builderFor(deleteFiles)
        .specsById(table().specs())
        .caseSensitive(isCaseSensitive())
        .scanMetrics(scanMetrics())
        .build();
  }

  private <T> T withJobGroupInfo(JobGroupInfo info, Supplier<T> supplier) {
    return JobGroupUtils.withJobGroupInfo(sparkContext, info, supplier);
  }

  private String jobDesc(String type) {
    List<String> options = Lists.newArrayList();
    options.add("snapshot_id=" + snapshot().snapshotId());
    String optionsAsString = COMMA.join(options);
    return String.format("Planning %s (%s) for %s", type, optionsAsString, table().name());
  }

  private List<ManifestFileBean> toBeans(List<ManifestFile> manifests) {
    return manifests.stream().map(ManifestFileBean::fromManifest).collect(Collectors.toList());
  }

  private Broadcast<Table> tableBroadcast() {
    if (tableBroadcast == null) {
      Table serializableTable = SerializableTableWithSize.copyOf(table());
      this.tableBroadcast = sparkContext.broadcast(serializableTable);
    }

    return tableBroadcast;
  }

  private <T> List<List<T>> collectPartitions(JavaRDD<T> rdd) {
    int[] partitionIds = IntStream.range(0, rdd.getNumPartitions()).toArray();
    return Arrays.asList(rdd.collectPartitions(partitionIds));
  }

  private int liveFilesCount(List<ManifestFile> manifests) {
    return manifests.stream().mapToInt(this::liveFilesCount).sum();
  }

  private int liveFilesCount(ManifestFile manifest) {
    return manifest.existingFilesCount() + manifest.addedFilesCount();
  }

  private static TableScanContext newTableScanContext(Table table) {
    if (table instanceof BaseTable) {
      MetricsReporter reporter = ((BaseTable) table).reporter();
      return ImmutableTableScanContext.builder().metricsReporter(reporter).build();
    } else {
      return TableScanContext.empty();
    }
  }

  private static class ReadDataManifest implements FlatMapFunction<ManifestFileBean, DataFile> {

    private final Broadcast<Table> table;
    private final Expression filter;
    private final boolean withStats;
    private final boolean isCaseSensitive;

    ReadDataManifest(Broadcast<Table> table, TableScanContext context, boolean withStats) {
      this.table = table;
      this.filter = context.rowFilter();
      this.withStats = withStats;
      this.isCaseSensitive = context.caseSensitive();
    }

    @Override
    public Iterator<DataFile> call(ManifestFileBean manifest) throws Exception {
      FileIO io = table.value().io();
      Map<Integer, PartitionSpec> specs = table.value().specs();
      return new ClosingIterator<>(
          ManifestFiles.read(manifest, io, specs)
              .select(withStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS)
              .filterRows(filter)
              .caseSensitive(isCaseSensitive)
              .iterator());
    }
  }

  private static class ReadDeleteManifest implements FlatMapFunction<ManifestFileBean, DeleteFile> {

    private final Broadcast<Table> table;
    private final Expression filter;
    private final boolean isCaseSensitive;

    ReadDeleteManifest(Broadcast<Table> table, TableScanContext context) {
      this.table = table;
      this.filter = context.rowFilter();
      this.isCaseSensitive = context.caseSensitive();
    }

    @Override
    public Iterator<DeleteFile> call(ManifestFileBean manifest) throws Exception {
      FileIO io = table.value().io();
      Map<Integer, PartitionSpec> specs = table.value().specs();
      return new ClosingIterator<>(
          ManifestFiles.readDeleteManifest(manifest, io, specs)
              .select(DELETE_SCAN_WITH_STATS_COLUMNS)
              .filterRows(filter)
              .caseSensitive(isCaseSensitive)
              .iterator());
    }
  }
}
