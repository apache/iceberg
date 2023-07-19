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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BaseDeleteOrphanFilesActionResult;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
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
 * only orphan data files. In addition, there is a way to configure an alternative delete method via
 * {@link #deleteWith(Consumer)}.
 *
 * <p><em>Note:</em> It is dangerous to call this action with a short retention interval as it might
 * corrupt the state of the table if another operation is writing at the same time.
 */
public class BaseDeleteOrphanFilesSparkAction
    extends BaseSparkAction<DeleteOrphanFiles, DeleteOrphanFiles.Result>
    implements DeleteOrphanFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDeleteOrphanFilesSparkAction.class);
  private static final Splitter COMMA = Splitter.on(",");
  private static final Map<String, String> EQUAL_SCHEMES_DEFAULT = ImmutableMap.of("s3n,s3a", "s3");
  private static final UserDefinedFunction filenameUDF =
      functions.udf(
          (String path) -> {
            int lastIndex = path.lastIndexOf(File.separator);
            if (lastIndex == -1) {
              return path;
            } else {
              return path.substring(lastIndex + 1);
            }
          },
          DataTypes.StringType);

  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE = null;

  private final SerializableConfiguration hadoopConf;
  private final int partitionDiscoveryParallelism;
  private final Table table;

  private Map<String, String> equalSchemes = flattenMap(EQUAL_SCHEMES_DEFAULT);
  private Map<String, String> equalAuthorities = Collections.emptyMap();
  private PrefixMismatchMode prefixMismatchMode = PrefixMismatchMode.ERROR;
  private String location = null;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Consumer<String> deleteFunc =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          table.io().deleteFile(file);
        }
      };

  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;

  public BaseDeleteOrphanFilesSparkAction(SparkSession spark, Table table) {
    super(spark);

    this.hadoopConf = new SerializableConfiguration(spark.sessionState().newHadoopConf());
    this.partitionDiscoveryParallelism =
        spark.sessionState().conf().parallelPartitionDiscoveryParallelism();
    this.table = table;
    this.location = table.location();

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot remove orphan files: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected DeleteOrphanFiles self() {
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction prefixMismatchMode(
      PrefixMismatchMode newPrefixMismatchMode) {
    this.prefixMismatchMode = newPrefixMismatchMode;
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction equalSchemes(Map<String, String> newEqualSchemes) {
    this.equalSchemes = Maps.newHashMap();
    equalSchemes.putAll(flattenMap(EQUAL_SCHEMES_DEFAULT));
    equalSchemes.putAll(flattenMap(newEqualSchemes));
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction equalAuthorities(
      Map<String, String> newEqualAuthorities) {
    this.equalAuthorities = Maps.newHashMap();
    equalAuthorities.putAll(flattenMap(newEqualAuthorities));
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  @Override
  public BaseDeleteOrphanFilesSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public DeleteOrphanFiles.Result execute() {
    JobGroupInfo info = newJobGroupInfo("REMOVE-ORPHAN-FILES", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private String jobDesc() {
    List<String> options = Lists.newArrayList();
    options.add("older_than=" + olderThanTimestamp);
    if (location != null) {
      options.add("location=" + location);
    }
    return String.format(
        "Removing orphan files (%s) from %s", Joiner.on(',').join(options), table.name());
  }

  private DeleteOrphanFiles.Result doExecute() {
    Dataset<Row> validDataFileDF = buildValidContentFileDF(table);
    Dataset<Row> validMetadataFileDF = buildValidMetadataFileDF(table);
    Dataset<Row> validFileDF = validDataFileDF.union(validMetadataFileDF);
    Dataset<Row> actualFileDF = buildActualFileDF();

    List<String> orphanFiles =
        findOrphanFiles(
            spark(), actualFileDF, validFileDF, equalSchemes, equalAuthorities, prefixMismatchMode);

    Tasks.foreach(orphanFiles)
        .noRetry()
        .executeWith(deleteExecutorService)
        .suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Failed to delete file: {}", file, exc))
        .run(deleteFunc::accept);

    return new BaseDeleteOrphanFilesActionResult(orphanFiles);
  }

  private Dataset<Row> buildActualFileDF() {
    List<String> subDirs = Lists.newArrayList();
    List<String> matchingFiles = Lists.newArrayList();

    Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

    // list at most 3 levels and only dirs that have less than 10 direct sub dirs on the driver
    listDirRecursively(location, predicate, hadoopConf.value(), 3, 10, subDirs, matchingFiles);

    JavaRDD<String> matchingFileRDD = sparkContext().parallelize(matchingFiles, 1);

    if (subDirs.isEmpty()) {
      return spark().createDataset(matchingFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
    }

    int parallelism = Math.min(subDirs.size(), partitionDiscoveryParallelism);
    JavaRDD<String> subDirRDD = sparkContext().parallelize(subDirs, parallelism);

    Broadcast<SerializableConfiguration> conf = sparkContext().broadcast(hadoopConf);
    JavaRDD<String> matchingLeafFileRDD =
        subDirRDD.mapPartitions(listDirsRecursively(conf, olderThanTimestamp));

    JavaRDD<String> completeMatchingFileRDD = matchingFileRDD.union(matchingLeafFileRDD);
    return spark()
        .createDataset(completeMatchingFileRDD.rdd(), Encoders.STRING())
        .toDF("file_path");
  }

  private static void listDirRecursively(
      String dir,
      Predicate<FileStatus> predicate,
      Configuration conf,
      int maxDepth,
      int maxDirectSubDirs,
      List<String> remainingSubDirs,
      List<String> matchingFiles) {

    // stop listing whenever we reach the max depth
    if (maxDepth <= 0) {
      remainingSubDirs.add(dir);
      return;
    }

    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf);

      List<String> subDirs = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, HiddenPathFilter.get())) {
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
            matchingFiles);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static FlatMapFunction<Iterator<String>, String> listDirsRecursively(
      Broadcast<SerializableConfiguration> conf, long olderThanTimestamp) {

    return dirs -> {
      List<String> subDirs = Lists.newArrayList();
      List<String> files = Lists.newArrayList();

      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

      int maxDepth = 2000;
      int maxDirectSubDirs = Integer.MAX_VALUE;

      dirs.forEachRemaining(
          dir -> {
            listDirRecursively(
                dir, predicate, conf.value().value(), maxDepth, maxDirectSubDirs, subDirs, files);
          });

      if (!subDirs.isEmpty()) {
        throw new RuntimeException(
            "Could not list subdirectories, reached maximum subdirectory depth: " + maxDepth);
      }

      return files.iterator();
    };
  }

  @VisibleForTesting
  static List<String> findOrphanFiles(
      SparkSession spark,
      Dataset<Row> actualFileDF,
      Dataset<Row> validFileDF,
      Map<String, String> equalSchemes,
      Map<String, String> equalAuthorities,
      PrefixMismatchMode prefixMismatchMode) {
    Dataset<FileMetadata> actualFileMetadataDS =
        actualFileDF.mapPartitions(
            toFileMetadata(equalSchemes, equalAuthorities), Encoders.bean(FileMetadata.class));
    Dataset<FileMetadata> validFileMetadataDS =
        validFileDF.mapPartitions(
            toFileMetadata(equalSchemes, equalAuthorities), Encoders.bean(FileMetadata.class));

    SetAccumulator<Pair<String, String>> conflicts = new SetAccumulator<>();
    spark.sparkContext().register(conflicts);

    Column joinCond = actualFileMetadataDS.col("path").equalTo(validFileMetadataDS.col("path"));

    List<String> orphanFiles =
        actualFileMetadataDS
            .joinWith(validFileMetadataDS, joinCond, "leftouter")
            .mapPartitions(findOrphanFiles(prefixMismatchMode, conflicts), Encoders.STRING())
            .collectAsList();

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

    return orphanFiles;
  }

  private static Map<String, String> flattenMap(Map<String, String> map) {
    Map<String, String> flattenedMap = Maps.newHashMap();
    if (map != null) {
      for (String key : map.keySet()) {
        String value = map.get(key);
        for (String splitKey : COMMA.split(key)) {
          flattenedMap.put(splitKey.trim(), value.trim());
        }
      }
    }
    return flattenedMap;
  }

  private static MapPartitionsFunction<Tuple2<FileMetadata, FileMetadata>, String> findOrphanFiles(
      PrefixMismatchMode mode, SetAccumulator<Pair<String, String>> conflicts) {
    return rows -> {
      Iterator<String> transformed =
          Iterators.transform(
              rows,
              row -> {
                FileMetadata actual = row._1;
                FileMetadata valid = row._2;

                if (valid == null) {
                  return actual.location;
                }

                boolean schemeMatch =
                    Strings.isNullOrEmpty(valid.scheme)
                        || valid.scheme.equalsIgnoreCase(actual.scheme);
                boolean authorityMatch =
                    Strings.isNullOrEmpty(valid.authority)
                        || valid.authority.equalsIgnoreCase(actual.authority);

                if ((!schemeMatch || !authorityMatch) && mode == PrefixMismatchMode.DELETE) {
                  return actual.location;
                } else {
                  if (!schemeMatch) {
                    conflicts.add(Pair.of(valid.scheme, actual.scheme));
                  }
                  if (!authorityMatch) {
                    conflicts.add(Pair.of(valid.authority, actual.authority));
                  }
                }

                return null;
              });
      return Iterators.filter(transformed, Objects::nonNull);
    };
  }

  private static MapPartitionsFunction<Row, FileMetadata> toFileMetadata(
      Map<String, String> equalSchemesMap, Map<String, String> equalAuthoritiesMap) {
    return rows ->
        Iterators.transform(
            rows,
            row -> {
              String location = row.getString(0);
              URI uri = new Path(location).toUri();
              String scheme = equalSchemesMap.getOrDefault(uri.getScheme(), uri.getScheme());
              String authority =
                  equalAuthoritiesMap.getOrDefault(uri.getAuthority(), uri.getAuthority());
              return new FileMetadata(scheme, authority, uri.getPath(), location);
            });
  }

  public static class FileMetadata implements Serializable {
    private String scheme;
    private String authority;
    private String path;
    private String location;

    public FileMetadata(String scheme, String authority, String path, String location) {
      this.scheme = scheme;
      this.authority = authority;
      this.path = path;
      this.location = location;
    }

    public FileMetadata() {}

    public void setScheme(String scheme) {
      this.scheme = scheme;
    }

    public void setAuthority(String authority) {
      this.authority = authority;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public void setLocation(String location) {
      this.location = location;
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

    public String getLocation() {
      return location;
    }
  }
}
