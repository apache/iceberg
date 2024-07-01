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

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ImmutableDeleteOrphanFiles;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * An action that removes orphan metadata, data and delete files by listing a given location and
 * comparing the actual files in that location with content and metadata files referenced by all
 * valid snapshots. The location must be accessible for listing via the Hadoop {@link FileSystem}.
 *
 * <p>By default, this action cleans up the table location returned by {@link Table#location()} and
 * removes unreachable files that are older than 3 days using {@link Table#io()}. The behavior can
 * be modified by passing a custom location to {@link #location} and a custom timestamp to {@link
 * #olderThan(long)}. For example, someone might point this action to the data folder to clean up
 * only orphan data files.
 *
 * <p>Configure an alternative delete method using {@link #deleteWith(Consumer)}.
 *
 * <p>For full control of the set of files being evaluated, use the {@link
 * #compareToFileList(Dataset)} argument. This skips the directory listing - any files in the
 * dataset provided which are not found in table metadata will be deleted, using the same {@link
 * Table#location()} and {@link #olderThan(long)} filtering as above.
 *
 * <p><em>Note:</em> It is dangerous to call this action with a short retention interval as it might
 * corrupt the state of the table if another operation is writing at the same time.
 */
public class DeleteOrphanFilesSparkAction extends BaseSparkAction<DeleteOrphanFilesSparkAction>
    implements DeleteOrphanFiles {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteOrphanFilesSparkAction.class);
  private static final Map<String, String> EQUAL_SCHEMES_DEFAULT = ImmutableMap.of("s3n,s3a", "s3");
  private static final int MAX_DRIVER_LISTING_DEPTH = 3;
  private static final int MAX_DRIVER_LISTING_DIRECT_SUB_DIRS = 10;
  private static final int MAX_EXECUTOR_LISTING_DEPTH = 2000;
  private static final int MAX_EXECUTOR_LISTING_DIRECT_SUB_DIRS = Integer.MAX_VALUE;

  private final SerializableConfiguration hadoopConf;
  private final int listingParallelism;
  private final Table table;
  private Map<String, String> equalSchemes = flattenMap(EQUAL_SCHEMES_DEFAULT);
  private Map<String, String> equalAuthorities = Collections.emptyMap();
  private PrefixMismatchMode prefixMismatchMode = PrefixMismatchMode.ERROR;
  private String location = null;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Dataset<Row> compareToFileList;
  private Consumer<String> deleteFunc = null;
  private ExecutorService deleteExecutorService = null;

  DeleteOrphanFilesSparkAction(SparkSession spark, Table table) {
    super(spark);

    this.hadoopConf = new SerializableConfiguration(spark.sessionState().newHadoopConf());
    this.listingParallelism = spark.sessionState().conf().parallelPartitionDiscoveryParallelism();
    this.table = table;
    this.location = table.location();

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot delete orphan files: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected DeleteOrphanFilesSparkAction self() {
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction prefixMismatchMode(PrefixMismatchMode newPrefixMismatchMode) {
    this.prefixMismatchMode = newPrefixMismatchMode;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction equalSchemes(Map<String, String> newEqualSchemes) {
    this.equalSchemes = Maps.newHashMap();
    equalSchemes.putAll(flattenMap(EQUAL_SCHEMES_DEFAULT));
    equalSchemes.putAll(flattenMap(newEqualSchemes));
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction equalAuthorities(Map<String, String> newEqualAuthorities) {
    this.equalAuthorities = Maps.newHashMap();
    equalAuthorities.putAll(flattenMap(newEqualAuthorities));
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  @Override
  public DeleteOrphanFilesSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  public DeleteOrphanFilesSparkAction compareToFileList(Dataset<Row> files) {
    StructType schema = files.schema();

    StructField filePathField = schema.apply(FILE_PATH);
    Preconditions.checkArgument(
        filePathField.dataType() == DataTypes.StringType,
        "Invalid %s column: %s is not a string",
        FILE_PATH,
        filePathField.dataType());

    StructField lastModifiedField = schema.apply(LAST_MODIFIED);
    Preconditions.checkArgument(
        lastModifiedField.dataType() == DataTypes.TimestampType,
        "Invalid %s column: %s is not a timestamp",
        LAST_MODIFIED,
        lastModifiedField.dataType());

    this.compareToFileList = files;
    return this;
  }

  private Dataset<String> filteredCompareToFileList() {
    Dataset<Row> files = compareToFileList;
    if (location != null) {
      files = files.filter(files.col(FILE_PATH).startsWith(location));
    }
    return files
        .filter(files.col(LAST_MODIFIED).lt(new Timestamp(olderThanTimestamp)))
        .select(files.col(FILE_PATH))
        .as(Encoders.STRING());
  }

  @Override
  public DeleteOrphanFiles.Result execute() {
    JobGroupInfo info = newJobGroupInfo("DELETE-ORPHAN-FILES", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();
    options.add("older_than=" + olderThanTimestamp);
    if (location != null) {
      options.add("location=" + location);
    }
    String optionsAsString = COMMA_JOINER.join(options);
    return String.format("Deleting orphan files (%s) from %s", optionsAsString, table.name());
  }

  private void deleteFiles(SupportsBulkOperations io, Set<String> paths) {
    try {
      io.deleteFiles(paths);
      LOG.info("Deleted {} files using bulk deletes", paths.size());
    } catch (BulkDeletionFailureException e) {
      int deletedFilesCount = paths.size() - e.numberFailedObjects();
      LOG.warn("Deleted only {} of {} files using bulk deletes", deletedFilesCount, paths.size());
    }
  }

  private DeleteOrphanFiles.Result doExecute() {
    Dataset<FileURI> actualFileIdentDS = actualFileIdentDS();
    Dataset<FileURI> validFileIdentDS = validFileIdentDS();

    Set<String> orphanFiles =
        findOrphanFiles(spark(), actualFileIdentDS, validFileIdentDS, prefixMismatchMode);

    if (deleteFunc == null && table.io() instanceof SupportsBulkOperations) {
      deleteFiles((SupportsBulkOperations) table.io(), orphanFiles);
    } else {

      Tasks.Builder<String> deleteTasks =
          Tasks.foreach(orphanFiles)
              .noRetry()
              .executeWith(deleteExecutorService)
              .suppressFailureWhenFinished()
              .onFailure((file, exc) -> LOG.warn("Failed to delete file: {}", file, exc));

      if (deleteFunc == null) {
        LOG.info(
            "Table IO {} does not support bulk operations. Using non-bulk deletes.",
            table.io().getClass().getName());
        deleteTasks.run(table.io()::deleteFile);
      } else {
        LOG.info("Custom delete function provided. Using non-bulk deletes");
        deleteTasks.run(deleteFunc::accept);
      }
    }

    return ImmutableDeleteOrphanFiles.Result.builder().orphanFileLocations(orphanFiles).build();
  }

  private Dataset<FileURI> validFileIdentDS() {
    // transform before union to avoid extra serialization/deserialization
    FileInfoToFileURI toFileURI = new FileInfoToFileURI(equalSchemes, equalAuthorities);

    Dataset<FileURI> contentFileIdentDS = toFileURI.apply(contentFileDS(table));
    Dataset<FileURI> manifestFileIdentDS = toFileURI.apply(manifestDS(table));
    Dataset<FileURI> manifestListIdentDS = toFileURI.apply(manifestListDS(table));
    Dataset<FileURI> otherMetadataFileIdentDS = toFileURI.apply(otherMetadataFileDS(table));

    return contentFileIdentDS
        .union(manifestFileIdentDS)
        .union(manifestListIdentDS)
        .union(otherMetadataFileIdentDS);
  }

  private Dataset<FileURI> actualFileIdentDS() {
    StringToFileURI toFileURI = new StringToFileURI(equalSchemes, equalAuthorities);
    if (compareToFileList == null) {
      return toFileURI.apply(listedFileDS());
    } else {
      return toFileURI.apply(filteredCompareToFileList());
    }
  }

  private Dataset<String> listedFileDS() {
    PatriciaTrie<String> subDirs = new PatriciaTrie<>();
    PatriciaTrie<String> matchingFiles = new PatriciaTrie<>();

    Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;
    PathFilter pathFilter = PartitionAwareHiddenPathFilter.forSpecs(table.specs());

    // list at most MAX_DRIVER_LISTING_DEPTH levels and only dirs that have
    // less than MAX_DRIVER_LISTING_DIRECT_SUB_DIRS direct sub dirs on the driver
    listDirRecursively(
        location,
        predicate,
        hadoopConf.value(),
        MAX_DRIVER_LISTING_DEPTH,
        MAX_DRIVER_LISTING_DIRECT_SUB_DIRS,
        subDirs,
        pathFilter,
        matchingFiles);

    JavaRDD<String> matchingFileRDD =
        sparkContext().parallelize(Lists.newArrayList(matchingFiles.keySet()), 1);

    if (subDirs.isEmpty()) {
      return spark().createDataset(matchingFileRDD.rdd(), Encoders.STRING());
    }

    int parallelism = Math.min(subDirs.size(), listingParallelism);
    JavaRDD<String> subDirRDD =
        sparkContext().parallelize(Lists.newArrayList(subDirs.keySet()), parallelism);

    Broadcast<SerializableConfiguration> conf = sparkContext().broadcast(hadoopConf);
    ListDirsRecursively listDirs = new ListDirsRecursively(conf, olderThanTimestamp, pathFilter);
    JavaRDD<String> matchingLeafFileRDD = subDirRDD.mapPartitions(listDirs);

    JavaRDD<String> completeMatchingFileRDD = matchingFileRDD.union(matchingLeafFileRDD);
    return spark().createDataset(completeMatchingFileRDD.rdd(), Encoders.STRING());
  }

  private static void listDirRecursively(
      String dir,
      Predicate<FileStatus> predicate,
      Configuration conf,
      int maxDepth,
      int maxDirectSubDirs,
      PatriciaTrie<String> remainingSubDirs,
      PathFilter pathFilter,
      PatriciaTrie<String> matchingFiles) {

    // stop listing whenever we reach the max depth
    if (maxDepth <= 0) {
      remainingSubDirs.put(dir, null);
      return;
    }

    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf);

      List<String> subDirs = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, pathFilter)) {
        if (file.isDirectory()) {
          subDirs.add(file.getPath().toString());
        } else if (file.isFile() && predicate.test(file)) {
          matchingFiles.put(file.getPath().toString(), null);
        }
      }

      // stop listing if the number of direct sub dirs is bigger than maxDirectSubDirs
      if (subDirs.size() > maxDirectSubDirs) {
        subDirs.forEach(subDir -> remainingSubDirs.put(subDir, null));
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

  @VisibleForTesting
  static Set<String> findOrphanFiles(
      SparkSession spark,
      Dataset<FileURI> actualFileIdentDS,
      Dataset<FileURI> validFileIdentDS,
      PrefixMismatchMode prefixMismatchMode) {

    SetAccumulator<Pair<String, String>> conflicts = new SetAccumulator<>();
    spark.sparkContext().register(conflicts);

    Column joinCond = actualFileIdentDS.col("path").equalTo(validFileIdentDS.col("path"));
    PatriciaTrie<String> files = new PatriciaTrie<>();
    Iterator<String> pathsIterator =
        actualFileIdentDS
            .joinWith(validFileIdentDS, joinCond, "leftouter")
            .mapPartitions(new FindOrphanFiles(prefixMismatchMode, conflicts), Encoders.STRING())
            .toLocalIterator();
    pathsIterator.forEachRemaining(path -> files.put(path, null));
    if (prefixMismatchMode == PrefixMismatchMode.ERROR && !conflicts.value().isEmpty()) {
      throw new ValidationException(
          "Unable to determine whether certain files are orphan. "
              + "Metadata references files that match listed/provided files except for authority/scheme. "
              + "Please, inspect the conflicting authorities/schemes and provide which of them are equal "
              + "by further configuring the action via equalSchemes() and equalAuthorities() methods. "
              + "Set the prefix mismatch mode to 'NONE' to ignore remaining locations with conflicting "
              + "authorities/schemes or to 'DELETE' iff you are ABSOLUTELY confident that remaining conflicting "
              + "authorities/schemes are different. It will be impossible to recover deleted files. "
              + "Conflicting authorities/schemes: %s.",
          conflicts.value());
    }

    return files.keySet();
  }

  private static Map<String, String> flattenMap(Map<String, String> map) {
    Map<String, String> flattenedMap = Maps.newHashMap();
    if (map != null) {
      for (String key : map.keySet()) {
        String value = map.get(key);
        for (String splitKey : COMMA_SPLITTER.split(key)) {
          flattenedMap.put(splitKey.trim(), value.trim());
        }
      }
    }
    return flattenedMap;
  }

  private static class ListDirsRecursively implements FlatMapFunction<Iterator<String>, String> {

    private final Broadcast<SerializableConfiguration> hadoopConf;
    private final long olderThanTimestamp;
    private final PathFilter pathFilter;

    ListDirsRecursively(
        Broadcast<SerializableConfiguration> hadoopConf,
        long olderThanTimestamp,
        PathFilter pathFilter) {

      this.hadoopConf = hadoopConf;
      this.olderThanTimestamp = olderThanTimestamp;
      this.pathFilter = pathFilter;
    }

    @Override
    public Iterator<String> call(Iterator<String> dirs) throws Exception {
      PatriciaTrie<String> subDirs = new PatriciaTrie<>();
      PatriciaTrie<String> files = new PatriciaTrie<>();
      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

      while (dirs.hasNext()) {
        listDirRecursively(
            dirs.next(),
            predicate,
            hadoopConf.value().value(),
            MAX_EXECUTOR_LISTING_DEPTH,
            MAX_EXECUTOR_LISTING_DIRECT_SUB_DIRS,
            subDirs,
            pathFilter,
            files);
      }

      if (!subDirs.isEmpty()) {
        throw new RuntimeException(
            "Could not list sub directories, reached maximum depth: " + MAX_EXECUTOR_LISTING_DEPTH);
      }

      return files.keySet().iterator();
    }
  }

  private static class FindOrphanFiles
      implements MapPartitionsFunction<Tuple2<FileURI, FileURI>, String> {

    private final PrefixMismatchMode mode;
    private final SetAccumulator<Pair<String, String>> conflicts;

    FindOrphanFiles(PrefixMismatchMode mode, SetAccumulator<Pair<String, String>> conflicts) {
      this.mode = mode;
      this.conflicts = conflicts;
    }

    @Override
    public Iterator<String> call(Iterator<Tuple2<FileURI, FileURI>> rows) throws Exception {
      Iterator<String> orphanFiles = Iterators.transform(rows, this::toOrphanFile);
      return Iterators.filter(orphanFiles, Objects::nonNull);
    }

    private String toOrphanFile(Tuple2<FileURI, FileURI> row) {
      FileURI actual = row._1;
      FileURI valid = row._2;

      if (valid == null) {
        return actual.uriAsString;
      }

      boolean schemeMatch = uriComponentMatch(valid.scheme, actual.scheme);
      boolean authorityMatch = uriComponentMatch(valid.authority, actual.authority);

      if ((!schemeMatch || !authorityMatch) && mode == PrefixMismatchMode.DELETE) {
        return actual.uriAsString;
      } else {
        if (!schemeMatch) {
          conflicts.add(Pair.of(valid.scheme, actual.scheme));
        }

        if (!authorityMatch) {
          conflicts.add(Pair.of(valid.authority, actual.authority));
        }

        return null;
      }
    }

    private boolean uriComponentMatch(String valid, String actual) {
      return Strings.isNullOrEmpty(valid) || valid.equalsIgnoreCase(actual);
    }
  }

  @VisibleForTesting
  static class StringToFileURI extends ToFileURI<String> {
    StringToFileURI(Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
      super(equalSchemes, equalAuthorities);
    }

    @Override
    protected String uriAsString(String input) {
      return input;
    }
  }

  @VisibleForTesting
  static class FileInfoToFileURI extends ToFileURI<FileInfo> {
    FileInfoToFileURI(Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
      super(equalSchemes, equalAuthorities);
    }

    @Override
    protected String uriAsString(FileInfo fileInfo) {
      return fileInfo.getPath();
    }
  }

  private abstract static class ToFileURI<I> implements MapPartitionsFunction<I, FileURI> {

    private final Map<String, String> equalSchemes;
    private final Map<String, String> equalAuthorities;

    ToFileURI(Map<String, String> equalSchemes, Map<String, String> equalAuthorities) {
      this.equalSchemes = equalSchemes;
      this.equalAuthorities = equalAuthorities;
    }

    protected abstract String uriAsString(I input);

    Dataset<FileURI> apply(Dataset<I> ds) {
      return ds.mapPartitions(this, FileURI.ENCODER);
    }

    @Override
    public Iterator<FileURI> call(Iterator<I> rows) throws Exception {
      return Iterators.transform(rows, this::toFileURI);
    }

    private FileURI toFileURI(I input) {
      String uriAsString = uriAsString(input);
      URI uri = new Path(uriAsString).toUri();
      String scheme = equalSchemes.getOrDefault(uri.getScheme(), uri.getScheme());
      String authority = equalAuthorities.getOrDefault(uri.getAuthority(), uri.getAuthority());
      return new FileURI(scheme, authority, uri.getPath(), uriAsString);
    }
  }

  /**
   * A {@link PathFilter} that filters out hidden path, but does not filter out paths that would be
   * marked as hidden by {@link HiddenPathFilter} due to a partition field that starts with one of
   * the characters that indicate a hidden path.
   */
  @VisibleForTesting
  static class PartitionAwareHiddenPathFilter implements PathFilter, Serializable {

    private final Set<String> hiddenPathPartitionNames;

    PartitionAwareHiddenPathFilter(Set<String> hiddenPathPartitionNames) {
      this.hiddenPathPartitionNames = hiddenPathPartitionNames;
    }

    @Override
    public boolean accept(Path path) {
      return isHiddenPartitionPath(path) || HiddenPathFilter.get().accept(path);
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

  public static class FileURI {
    public static final Encoder<FileURI> ENCODER = Encoders.bean(FileURI.class);

    private String scheme;
    private String authority;
    private String path;
    private String uriAsString;

    public FileURI(String scheme, String authority, String path, String uriAsString) {
      this.scheme = scheme;
      this.authority = authority;
      this.path = path;
      this.uriAsString = uriAsString;
    }

    public FileURI() {}

    public void setScheme(String scheme) {
      this.scheme = scheme;
    }

    public void setAuthority(String authority) {
      this.authority = authority;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public void setUriAsString(String uriAsString) {
      this.uriAsString = uriAsString;
    }

    public String getScheme() {
      return scheme;
    }

    public String getAuthority() {
      return authority;
    }

    public String getPath() {
      return path;
    }

    public String getUriAsString() {
      return uriAsString;
    }
  }
}
