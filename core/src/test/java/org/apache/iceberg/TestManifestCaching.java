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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.testing.GcFinalization;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.ContentCache;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestManifestCaching {

  // Schema passed to create tables
  static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  @TempDir private Path temp;

  @Test
  public void testPlanWithCache() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            HadoopFileIO.class.getName(),
            CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
            "true");
    Table table = createTable(properties);
    ContentCache cache = ManifestFiles.contentCache(table.io());
    assertThat(cache.estimatedCacheSize()).isEqualTo(0);

    int numFiles = 4;
    List<DataFile> files16Mb = newFiles(numFiles, 16 * 1024 * 1024);
    appendFiles(files16Mb, table);

    // planTask with SPLIT_SIZE half of the file size
    TableScan scan1 =
        table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(8 * 1024 * 1024));
    assertThat(scan1.planTasks()).hasSize(numFiles * 2);
    assertThat(cache.estimatedCacheSize())
        .as("All manifest files should be cached")
        .isEqualTo(numFiles);
    assertThat(cache.stats().loadCount())
        .as("All manifest files should be recently loaded")
        .isEqualTo(numFiles);
    long missCount = cache.stats().missCount();

    // planFiles and verify that cache size still the same
    TableScan scan2 = table.newScan();
    assertThat(scan2.planFiles()).hasSize(numFiles);
    assertThat(cache.estimatedCacheSize()).isEqualTo(numFiles);
    assertThat(cache.stats().missCount())
        .as("All manifest file reads should hit cache")
        .isEqualTo(missCount);

    ManifestFiles.dropCache(table.io());
  }

  @Test
  public void testPlanWithSmallCache() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName(),
            CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true",
            CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, "1",
            CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, "1");
    Table table = createTable(properties);

    int numFiles = 4;
    List<DataFile> files16Mb = newFiles(numFiles, 16 * 1024 * 1024);
    appendFiles(files16Mb, table);

    // We should never hit cache.
    TableScan scan = table.newScan();
    ContentCache cache = ManifestFiles.contentCache(scan.table().io());
    assertThat(cache.maxContentLength()).isEqualTo(1);
    assertThat(cache.maxTotalBytes()).isEqualTo(1);
    assertThat(scan.planFiles()).hasSize(numFiles);
    assertThat(cache.estimatedCacheSize()).isEqualTo(0);
    assertThat(cache.stats().loadCount())
        .as("File should not be loaded through cache")
        .isEqualTo(0);
    assertThat(cache.stats().requestCount()).as("Cache should not serve file").isEqualTo(0);
    ManifestFiles.dropCache(scan.table().io());
  }

  @Test
  public void testUniqueCache() throws Exception {
    Map<String, String> properties1 =
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            HadoopFileIO.class.getName(),
            CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
            "true");
    Table table1 = createTable(properties1);

    Map<String, String> properties2 =
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName(),
            CatalogProperties.IO_MANIFEST_CACHE_ENABLED, "true",
            CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES, "1",
            CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH, "1");
    Table table2 = createTable(properties2);

    ContentCache cache1 = ManifestFiles.contentCache(table1.io());
    ContentCache cache2 = ManifestFiles.contentCache(table2.io());
    ContentCache cache3 = ManifestFiles.contentCache(table2.io());
    assertThat(cache2).isNotSameAs(cache1);
    assertThat(cache3).isSameAs(cache2);

    ManifestFiles.dropCache(table1.io());
    ManifestFiles.dropCache(table2.io());
  }

  @Test
  public void testRecreateCache() throws Exception {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            HadoopFileIO.class.getName(),
            CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
            "true");
    Table table = createTable(properties);

    ContentCache cache1 = ManifestFiles.contentCache(table.io());
    ManifestFiles.dropCache(table.io());

    ContentCache cache2 = ManifestFiles.contentCache(table.io());
    assertThat(cache2).isNotSameAs(cache1);
    ManifestFiles.dropCache(table.io());
  }

  @Test
  public void testWeakFileIOReferenceCleanUp() {
    Cache<FileIO, ContentCache> manifestCache =
        ManifestFiles.newManifestCacheBuilder().executor(Runnable::run).build();
    int maxIO = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.defaultValue();
    FileIO firstIO = null;
    ContentCache firstCache = null;
    for (int i = 0; i < maxIO - 1; i++) {
      FileIO io = cacheEnabledHadoopFileIO();
      ContentCache cache = contentCache(manifestCache, io);
      if (i == 0) {
        firstIO = io;
        firstCache = cache;
      }
    }

    // Insert the last FileIO and trigger GC + cleanup.
    FileIO lastIO = cacheEnabledHadoopFileIO();
    ContentCache lastCache = contentCache(manifestCache, lastIO);
    GcFinalization.awaitDone(
        () -> {
          manifestCache.cleanUp();
          return manifestCache.estimatedSize() == 2;
        });

    // Verify that manifestCache evicts all FileIO except the firstIO and lastIO.
    ContentCache cache1 = contentCache(manifestCache, firstIO);
    ContentCache cacheN = contentCache(manifestCache, lastIO);
    assertThat(cache1).isSameAs(firstCache);
    assertThat(cacheN).isSameAs(lastCache);
    assertThat(manifestCache.stats().loadCount()).isEqualTo(maxIO);
    assertThat(manifestCache.stats().evictionCount()).isEqualTo(maxIO - 2);
  }

  /**
   * Helper to get existing or insert new {@link ContentCache} into the given manifestCache.
   *
   * @return an existing or new {@link ContentCache} associated with given io.
   */
  private static ContentCache contentCache(Cache<FileIO, ContentCache> manifestCache, FileIO io) {
    return manifestCache.get(
        io,
        fileIO ->
            new ContentCache(
                ManifestFiles.cacheDurationMs(fileIO),
                ManifestFiles.cacheTotalBytes(fileIO),
                ManifestFiles.cacheMaxContentLength(fileIO)));
  }

  private FileIO cacheEnabledHadoopFileIO() {
    Map<String, String> properties =
        ImmutableMap.of(
            CatalogProperties.FILE_IO_IMPL,
            HadoopFileIO.class.getName(),
            CatalogProperties.IO_MANIFEST_CACHE_ENABLED,
            "true");
    HadoopFileIO io = new HadoopFileIO(new Configuration());
    io.initialize(properties);
    return io;
  }

  private Table createTable(Map<String, String> properties) throws Exception {
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    return hadoopCatalog(properties)
        .buildTable(tableIdent, SCHEMA)
        .withPartitionSpec(SPEC)
        .create();
  }

  private HadoopCatalog hadoopCatalog(Map<String, String> catalogProperties) throws IOException {
    HadoopCatalog hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(new Configuration());
    hadoopCatalog.initialize(
        "hadoop",
        ImmutableMap.<String, String>builder()
            .putAll(catalogProperties)
            .put(
                CatalogProperties.WAREHOUSE_LOCATION,
                Files.createTempDirectory(temp, "junit").toFile().getAbsolutePath())
            .buildOrThrow());
    return hadoopCatalog;
  }

  private void appendFiles(Iterable<DataFile> files, Table table) {
    for (DataFile file : files) {
      AppendFiles appendFile = table.newAppend();
      appendFile.appendFile(file);
      appendFile.commit();
    }
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes) {
    return newFiles(numFiles, sizeInBytes, FileFormat.PARQUET, 1);
  }

  private List<DataFile> newFiles(
      int numFiles, long sizeInBytes, FileFormat fileFormat, int numOffset) {
    List<DataFile> files = Lists.newArrayList();
    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      files.add(newFile(sizeInBytes, fileFormat, numOffset));
    }
    return files;
  }

  private DataFile newFile(long sizeInBytes, FileFormat fileFormat, int numOffsets) {
    String fileName = UUID.randomUUID().toString();
    DataFiles.Builder builder =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(fileFormat.addExtension(fileName))
            .withFileSizeInBytes(sizeInBytes)
            .withRecordCount(2);

    if (numOffsets > 1) {
      long stepSize = sizeInBytes / numOffsets;
      List<Long> offsets =
          LongStream.range(0, numOffsets)
              .map(i -> i * stepSize)
              .boxed()
              .collect(Collectors.toList());
      builder.withSplitOffsets(offsets);
    }

    return builder.build();
  }
}
