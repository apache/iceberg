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
package org.apache.iceberg.flink.actions;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ImmutableDeleteOrphanFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Flink action to remove orphan files from a table. */
public class CleanOrphanFilesAction implements DeleteOrphanFiles {
  private static final Logger LOG = LoggerFactory.getLogger(CleanOrphanFilesAction.class);

  private static final String ACCUMULATOR_CONFLICTS_NAME = "conflicts";
  private static final int MAX_DRIVER_LISTING_DEPTH = 3;
  private static final int MAX_DRIVER_LISTING_DIRECT_SUB_DIRS = 10;
  private static final int MAX_EXECUTOR_LISTING_DEPTH = 2000;
  private static final int MAX_EXECUTOR_LISTING_DIRECT_SUB_DIRS = Integer.MAX_VALUE;

  private static final Splitter COMMA_SPLITTER = Splitter.on(',');
  private static final Map<String, String> EQUAL_SCHEMES_DEFAULT = ImmutableMap.of("s3n,s3a", "s3");

  private final StreamExecutionEnvironment env;
  private int maxParallelism;
  private final Table table;
  private final SerializableConfiguration hadoopConf;

  private static Map<String, String> equalSchemes = flattenMap(EQUAL_SCHEMES_DEFAULT);
  private static Map<String, String> equalAuthorities = Collections.emptyMap();
  private PrefixMismatchMode prefixMismatchMode = PrefixMismatchMode.ERROR;
  private String location;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;

  public CleanOrphanFilesAction(StreamExecutionEnvironment env, Table table) {
    this.env = env;
    this.maxParallelism = env.getParallelism();
    this.table = table;
    Configuration conf = new Configuration();
    table.properties().forEach(conf::set);
    this.hadoopConf = new SerializableConfiguration(conf);
    this.location = table.location();
  }

  @Override
  public CleanOrphanFilesAction prefixMismatchMode(PrefixMismatchMode mode) {
    this.prefixMismatchMode = mode;
    return this;
  }

  @Override
  public CleanOrphanFilesAction equalSchemes(Map<String, String> newEqualSchemes) {
    this.equalSchemes = Maps.newHashMap();
    equalSchemes.putAll(flattenMap(EQUAL_SCHEMES_DEFAULT));
    equalSchemes.putAll(flattenMap(newEqualSchemes));
    return this;
  }

  @Override
  public CleanOrphanFilesAction equalAuthorities(Map<String, String> newEqualAuthorities) {
    this.equalAuthorities = Maps.newHashMap();
    this.equalAuthorities.putAll(newEqualAuthorities);
    return this;
  }

  @Override
  public CleanOrphanFilesAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  @Override
  public CleanOrphanFilesAction olderThan(long newTimestamp) {
    this.olderThanTimestamp = newTimestamp;
    return this;
  }

  @Override
  public CleanOrphanFilesAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public CleanOrphanFilesAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public Result execute() {
    try {
      // 1. collect valid files, include datafile、manifest、manifest list、others metadata files.
      DataStream<NormalizedPath> validFiles = collectValidFiles();

      // 2. collect actual files, include all files.
      DataStream<NormalizedPath> actualFiles = collectActualFiles();
      if (actualFiles == null) {
        return ImmutableDeleteOrphanFiles.Result.builder()
            .orphanFileLocations(ImmutableList.of())
            .build();
      }

      // 3. find orphan files.
      List<String> orphanFiles = findOrphanFiles(validFiles, actualFiles, prefixMismatchMode);

      // 4. delete orphan files
      executeDeleteFiles(orphanFiles);

      // 5. return result.
      return ImmutableDeleteOrphanFiles.Result.builder().orphanFileLocations(orphanFiles).build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete orphan files", e);
    }
  }

  @VisibleForTesting
  static List<String> findOrphanFiles(
      DataStream<NormalizedPath> validFiles,
      DataStream<NormalizedPath> actualFiles,
      PrefixMismatchMode prefixMismatchMode)
      throws Exception {
    DataStream<Tuple3<String, String, String>> orphanFilesStream =
        actualFiles
            .keyBy(NormalizedPath::getPath)
            .connect(validFiles.keyBy(NormalizedPath::getPath))
            .process(new FindOrphanFilesProcessor(prefixMismatchMode))
            .name("Find orphan files");

    ClientAndIterator<Tuple3<String, String, String>> collectWithClient =
        executeAndCollectWithClient(orphanFilesStream, "Collect clean orphan files.");
    JobExecutionResult result = collectWithClient.client.getJobExecutionResult().get();
    List<Pair<String, String>> conflicts = result.getAccumulatorResult(ACCUMULATOR_CONFLICTS_NAME);

    if (prefixMismatchMode == PrefixMismatchMode.ERROR
        && conflicts != null
        && !conflicts.isEmpty()) {
      throw new ValidationException("Conflicting authorities/schemes: %s.", conflicts);
    }

    CollectOrphanFiles collectOrphanFiles = new CollectOrphanFiles();
    CloseableIterator<Tuple3<String, String, String>> iterator = collectWithClient.iterator;
    while (iterator.hasNext()) {
      collectOrphanFiles.merge(iterator.next());
    }
    return collectOrphanFiles.getResult();
  }

  static ClientAndIterator<Tuple3<String, String, String>> executeAndCollectWithClient(
      DataStream<Tuple3<String, String, String>> dataStream, String jobExecutionName)
      throws Exception {
    final CloseableIterator<Tuple3<String, String, String>> iterator = dataStream.collectAsync();
    final JobClient jobClient = dataStream.getExecutionEnvironment().executeAsync(jobExecutionName);
    return new ClientAndIterator<>(jobClient, iterator);
  }

  static class FindOrphanFilesProcessor
      extends KeyedCoProcessFunction<
          Object, NormalizedPath, NormalizedPath, Tuple3<String, String, String>> {
    private transient MapState<String, NormalizedPath> left;
    private transient MapState<String, NormalizedPath> right;
    private final ListAccumulator<Pair<String, String>> conflicts = new ListAccumulator<>();
    private final PrefixMismatchMode mode;

    FindOrphanFilesProcessor(PrefixMismatchMode prefixMismatchMode) {
      this.mode = prefixMismatchMode;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
      super.open(parameters);
      this.left =
          getRuntimeContext()
              .getMapState(
                  new MapStateDescriptor<String, NormalizedPath>(
                      "left", String.class, NormalizedPath.class));
      this.right =
          getRuntimeContext()
              .getMapState(
                  new MapStateDescriptor<String, NormalizedPath>(
                      "right", String.class, NormalizedPath.class));
      getRuntimeContext().addAccumulator(ACCUMULATOR_CONFLICTS_NAME, conflicts);
    }

    @Override
    public void processElement1(
        NormalizedPath actual,
        KeyedCoProcessFunction<
                    Object, NormalizedPath, NormalizedPath, Tuple3<String, String, String>>
                .Context
            ctx,
        Collector<Tuple3<String, String, String>> out)
        throws Exception {
      NormalizedPath valid = right.get(actual.path);
      if (valid != null) {
        retract(actual, valid, out);
      } else {
        out.collect(Tuple3.of(actual.path, null, actual.uriAsString));
      }
      left.put(actual.path, actual);
    }

    @Override
    public void processElement2(
        NormalizedPath valid,
        KeyedCoProcessFunction<
                    Object, NormalizedPath, NormalizedPath, Tuple3<String, String, String>>
                .Context
            ctx,
        Collector<Tuple3<String, String, String>> out)
        throws Exception {
      NormalizedPath actual = left.get(valid.path);
      if (actual != null) {
        retract(actual, valid, out);
      }
      right.put(valid.path, valid);
    }

    private void retract(
        NormalizedPath actual,
        NormalizedPath valid,
        Collector<Tuple3<String, String, String>> out) {
      // if the file is not in the valid files, it is an orphan file.
      boolean schemeMatch = uriComponentMatch(valid.scheme, actual.scheme);
      boolean authorityMatch = uriComponentMatch(valid.authority, actual.authority);
      if ((!schemeMatch || !authorityMatch) && mode == PrefixMismatchMode.DELETE) {
        // if the scheme/authority mismatch, we should delete the actual file.
        out.collect(Tuple3.of(actual.getPath(), null, actual.uriAsString));
      } else {
        if (!schemeMatch) {
          conflicts.add(Pair.of(valid.scheme, actual.scheme));
        }
        if (!authorityMatch) {
          conflicts.add(Pair.of(valid.authority, actual.authority));
        }
        out.collect(Tuple3.of(actual.getPath(), valid.getPath(), actual.uriAsString));
      }
    }

    private boolean uriComponentMatch(String valid, String actual) {
      return Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual);
    }
  }

  static class CollectOrphanFiles {
    private Map<String, String> noMatch = Maps.newConcurrentMap();

    public void merge(Tuple3<String, String, String> value) {
      if (value.f1 == null) {
        noMatch.put(value.f0, value.f2);
      } else {
        noMatch.remove(value.f0);
      }
    }

    public List<String> getResult() {
      return Lists.newArrayList(noMatch.values());
    }
  }

  private void executeDeleteFiles(List<String> orphanFiles) {
    if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
      ((SupportsBulkOperations) table.io()).deleteFiles(orphanFiles);
      LOG.info("Deleted {} files using bulk deletes", orphanFiles.size());
    } else {
      Tasks.Builder<String> deleteTasks =
          Tasks.foreach(orphanFiles)
              .noRetry()
              .executeWith(deleteExecutorService)
              .suppressFailureWhenFinished()
              .onFailure((file, exc) -> LOG.warn("Failed to delete file: {}", file, exc));
      if (deleteFunc == null) {
        deleteTasks.run(table.io()::deleteFile);
      } else {
        LOG.info("Custom delete function provided. Using non-bulk deletes");
        deleteTasks.run(deleteFunc::accept);
      }
    }
  }

  private DataStream<NormalizedPath> collectValidFiles() {
    return getContentFiles().union(getManifestLists()).union(getOthersMetadataFiles());
  }

  private DataStream<NormalizedPath> getContentFiles() {
    Table metadataTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.ALL_MANIFESTS);
    CloseableIterable<FileScanTask> fileScanTasks = metadataTable.newScan().planFiles();

    int fileCountEstimate =
        (int) Math.ceil(fileScanTasks.spliterator().getExactSizeIfKnown() / 50.0);
    int dynamicParallelism = Math.min(Math.max(fileCountEstimate, 1), maxParallelism);

    Schema schema = metadataTable.schema();
    List<FileScanTask> newFileScanTasks =
        Lists.newArrayList(fileScanTasks).stream().collect(Collectors.toList());
    return env.fromCollection(newFileScanTasks)
        .process(
            new ProcessFunction<FileScanTask, ManifestFileBean>() {
              @Override
              public void processElement(
                  FileScanTask task,
                  ProcessFunction<FileScanTask, ManifestFileBean>.Context ctx,
                  Collector<ManifestFileBean> out) {
                if (task.isDataTask()) {
                  CloseableIterable<StructLike> rows = task.asDataTask().rows();
                  for (StructLike row : rows) {
                    // Add manifest files.
                    ManifestFileBean manifest = ManifestFileBean.fromRowData(row, schema);
                    out.collect(manifest);
                  }
                }
              }
            })
        .setParallelism(dynamicParallelism)
        .process(
            new ProcessFunction<ManifestFileBean, NormalizedPath>() {
              @Override
              public void processElement(
                  ManifestFileBean manifest,
                  ProcessFunction<ManifestFileBean, NormalizedPath>.Context ctx,
                  Collector<NormalizedPath> out) {
                // Discovery data files.
                NormalizedPath manifestNormalizedPath = normalizePath(manifest.path());
                out.collect(manifestNormalizedPath);
                Set<NormalizedPath> contentNormalizedPaths =
                    extractContentFiles(metadataTable, manifest);
                for (NormalizedPath contentNormalizedPath : contentNormalizedPaths) {
                  out.collect(contentNormalizedPath);
                }
              }
            })
        .setParallelism(dynamicParallelism * 2);
  }

  // extract content files
  private static Set<NormalizedPath> extractContentFiles(
      Table metadataTable, ManifestFileBean manifest) {

    FileIO io = metadataTable.io();
    Map<Integer, PartitionSpec> specs = metadataTable.specs();
    Set<NormalizedPath> normalizedPaths = Sets.newHashSet();
    ManifestContent content = manifest.content();
    List<String> proj = ImmutableList.of(DataFile.FILE_PATH.name(), DataFile.CONTENT.name());
    switch (content) {
      case DATA:
        ManifestFiles.read(manifest, io, specs)
            .select(proj)
            .iterator()
            .forEachRemaining(
                file -> {
                  NormalizedPath normalizedPath = normalizePath(file.location());
                  normalizedPaths.add(normalizedPath);
                });
        break;
      case DELETES:
        ManifestFiles.readDeleteManifest(manifest, io, specs)
            .select(proj)
            .iterator()
            .forEachRemaining(
                file -> {
                  NormalizedPath normalizedPath = normalizePath(file.location());
                  normalizedPaths.add(normalizedPath);
                });
        break;
      default:
        throw new IllegalArgumentException("Unsupported manifest content type:" + content);
    }
    return normalizedPaths;
  }

  private DataStream<NormalizedPath> getManifestLists() {
    List<String> manifestLists = ReachableFileUtil.manifestListLocations(table);
    Set<NormalizedPath> normalizedPaths = Sets.newHashSet();
    for (String manifestListLocation : manifestLists) {
      NormalizedPath normalizedPath = normalizePath(manifestListLocation);
      normalizedPaths.add(normalizedPath);
    }
    return env.fromCollection(normalizedPaths);
  }

  private DataStream<NormalizedPath> getOthersMetadataFiles() {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.addAll(ReachableFileUtil.metadataFileLocations(table, false));
    otherMetadataFiles.add(ReachableFileUtil.versionHintLocation(table));
    otherMetadataFiles.addAll(ReachableFileUtil.statisticsFilesLocations(table));
    Set<NormalizedPath> result = Sets.newHashSet();
    for (String metadataFile : otherMetadataFiles) {
      if (metadataFile == null) {
        continue;
      }
      NormalizedPath normalizedPath = normalizePath(metadataFile);
      result.add(normalizedPath);
    }
    return env.fromCollection(result);
  }

  private DataStream<NormalizedPath> collectActualFiles() {
    List<String> subDirs = Lists.newArrayList();
    List<String> matchingFiles = Lists.newArrayList();

    Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;
    PathFilter pathFilter = PartitionAwareHiddenPathFilter.forSpecs(table.specs());

    // list at most MAX_DRIVER_LISTING_DEPTH levels and only dirs that have
    // less than MAX_DRIVER_LISTING_DIRECT_SUB_DIRS direct sub dirs on the driver
    listDirRecursively(
        location,
        predicate,
        hadoopConf,
        MAX_DRIVER_LISTING_DEPTH,
        MAX_DRIVER_LISTING_DIRECT_SUB_DIRS,
        subDirs,
        pathFilter,
        matchingFiles);

    if (matchingFiles.isEmpty()) {
      return null;
    }
    DataStream<NormalizedPath> dataStream =
        env.fromCollection(matchingFiles).map(normalizedPathMapFunction());

    if (subDirs.isEmpty()) {
      return dataStream;
    }
    DataStream<NormalizedPath> recursiveStream =
        env.fromCollection(subDirs)
            .flatMap(new RecursiveFileScanner(hadoopConf, olderThanTimestamp, pathFilter))
            .map(normalizedPathMapFunction())
            .setParallelism(Math.min(maxParallelism, subDirs.size()))
            .name("file-scanner");
    return dataStream.union(recursiveStream);
  }

  private static @NotNull MapFunction<String, NormalizedPath> normalizedPathMapFunction() {
    return new MapFunction<String, NormalizedPath>() {
      @Override
      public NormalizedPath map(String value) throws Exception {
        return normalizePath(value);
      }
    };
  }

  private static void listDirRecursively(
      String dir,
      Predicate<FileStatus> predicate,
      SerializableConfiguration conf,
      int maxDepth,
      int maxDirectSubDirs,
      List<String> remainingSubDirs,
      PathFilter pathFilter,
      List<String> matchingFiles) {

    // stop listing whenever we reach the max depth
    if (maxDepth <= 0) {
      remainingSubDirs.add(dir);
      return;
    }

    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf.get());

      List<String> subDirs = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, pathFilter)) {
        if (file.isDirectory()) {
          subDirs.add(file.getPath().toString());
        } else if (file.isFile() && predicate.test(file)) {
          matchingFiles.add(file.getPath().toString());
        }
      }

      // stop listing if the number of direct sub dirs is bigger than maxDirectSubDirs
      if (subDirs.size() > maxDirectSubDirs) {
        remainingSubDirs.addAll(subDirs);
        return;
      }

      for (String subDir : subDirs) {
        listDirRecursively(
            subDir,
            predicate,
            conf,
            maxDepth - 1,
            maxDirectSubDirs,
            remainingSubDirs,
            pathFilter,
            matchingFiles);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Map<String, String> flattenMap(Map<String, String> input) {
    Map<String, String> result = Maps.newHashMap();
    input.forEach(
        (key, value) ->
            COMMA_SPLITTER.splitToList(key).forEach(k -> result.put(k.trim(), value.trim())));
    return result;
  }

  static class RecursiveFileScanner extends RichFlatMapFunction<String, String> {
    private final SerializableConfiguration hadoopConf;
    private final long olderThanTimestamp;
    private final PathFilter pathFilter;

    RecursiveFileScanner(
        SerializableConfiguration hadoopConf, long olderThanTimestamp, PathFilter pathFilter) {
      this.hadoopConf = hadoopConf;
      this.olderThanTimestamp = olderThanTimestamp;
      this.pathFilter = pathFilter;
    }

    @Override
    public void flatMap(String dir, Collector<String> out) throws Exception {
      List<String> subDirs = Lists.newArrayList();
      List<String> files = Lists.newArrayList();
      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;
      listDirRecursively(
          dir,
          predicate,
          hadoopConf,
          MAX_EXECUTOR_LISTING_DEPTH,
          MAX_EXECUTOR_LISTING_DIRECT_SUB_DIRS,
          subDirs,
          pathFilter,
          files);
      if (!subDirs.isEmpty()) {
        throw new RuntimeException(
            "Could not list sub directories, reached maximum depth: " + MAX_EXECUTOR_LISTING_DEPTH);
      }
      for (String file : files) {
        out.collect(file);
      }
    }
  }

  @VisibleForTesting
  static class NormalizedPath implements Serializable {
    private final String scheme;
    private final String authority;
    private final String path;
    private final String uriAsString;

    NormalizedPath(String scheme, String authority, String path, String uriAsString) {
      this.scheme = scheme;
      this.authority = authority;
      this.path = path;
      this.uriAsString = uriAsString;
    }

    public String getPath() {
      return path;
    }
  }

  @VisibleForTesting
  static NormalizedPath normalizePath(String uriString) {
    URI uri = new Path(uriString).toUri();
    String scheme = equalSchemes.getOrDefault(uri.getScheme(), uri.getScheme());
    String authority = equalAuthorities.getOrDefault(uri.getAuthority(), uri.getAuthority());
    return new NormalizedPath(scheme, authority, uri.getPath(), uriString);
  }

  @VisibleForTesting
  static NormalizedPath normalizePath(
      Map<String, String> schemes, Map<String, String> authorities, String uriString) {
    URI uri = new Path(uriString).toUri();
    String scheme = schemes.getOrDefault(uri.getScheme(), uri.getScheme());
    String authority = authorities.getOrDefault(uri.getAuthority(), uri.getAuthority());
    return new NormalizedPath(scheme, authority, uri.getPath(), uriString);
  }

  static class PartitionAwareHiddenPathFilter implements PathFilter, Serializable {

    private final Set<String> hiddenPathPartitionNames;

    PartitionAwareHiddenPathFilter(Set<String> hiddenPathPartitionNames) {
      this.hiddenPathPartitionNames = hiddenPathPartitionNames;
    }

    @Override
    public boolean accept(Path path) {
      boolean isHiddenPartition = isHiddenPartitionPath(path);
      boolean isHiddenFile = HiddenPathFilter.get().accept(path);
      return isHiddenPartition || isHiddenFile;
    }

    private boolean isHiddenPartitionPath(Path path) {
      return hiddenPathPartitionNames.stream().anyMatch(path.getName()::startsWith);
    }

    static PathFilter forSpecs(Map<Integer, PartitionSpec> specs) {
      if (specs == null) {
        return HiddenPathFilter.get();
      }

      Set<String> partitionNames =
          specs.values().stream()
              .map(PartitionSpec::fields)
              .flatMap(List::stream)
              .filter(field -> field.name().startsWith("_") || field.name().startsWith("."))
              .map(field -> field.name() + "=")
              .collect(Collectors.toSet());

      if (partitionNames.isEmpty()) {
        return HiddenPathFilter.get();
      } else {
        return new PartitionAwareHiddenPathFilter(partitionNames);
      }
    }
  }
}
