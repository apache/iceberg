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
package org.apache.iceberg.io.canonicaluri;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCanonicalUriFileIOV3 {
  private static final String TABLE_LOCATION = "s3://warehouse/test-table";

  @TempDir private Path temp;

  @Test
  void keepsV3DataDeletionVectorAndMetadataLocationsCanonical() throws Exception {
    Files.createDirectories(temp.resolve("test-table/data"));
    Files.createDirectories(temp.resolve("test-table/metadata"));
    CanonicalUriFileIO io = newFileIO();
    InMemoryCatalogTableOperations ops = new InMemoryCatalogTableOperations(io);
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(TableProperties.FORMAT_VERSION, "3");
    tableProperties.put(
        TableProperties.WRITE_LOCATION_PROVIDER_IMPL, CanonicalLocationProvider.class.getName());
    tableProperties.put(PathAliasProperties.LOCATION_PROVIDER_CANONICAL_PREFIX, "s3://warehouse/");

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            TABLE_LOCATION,
            tableProperties);
    ops.commit(null, metadata);
    Table table = new BaseTable(ops, "path-alias-v3");

    String dataLocation = table.locationProvider().newDataLocation("data.parquet");
    byte[] dataBytes = "ten logical rows".getBytes(UTF_8);
    try (PositionOutputStream output = table.io().newOutputFile(dataLocation).create()) {
      output.write(dataBytes);
    }

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(dataLocation)
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(dataBytes.length)
            .withRecordCount(10L)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, ignored -> null);
    try (DVFileWriter closeableWriter = writer) {
      closeableWriter.delete(dataLocation, 0L, table.spec(), (StructLike) null);
    }

    DeleteFile deletionVector = only(writer.result().deleteFiles());
    table.newRowDelta().addDeletes(deletionVector).commit();

    assertThat(ops.current().formatVersion()).isEqualTo(3);
    assertThat(ops.currentMetadataLocation()).startsWith(TABLE_LOCATION + "/metadata/");
    assertThat(table.currentSnapshot().manifestListLocation()).startsWith(TABLE_LOCATION);
    assertThat(table.currentSnapshot().allManifests(table.io()))
        .extracting(ManifestFile::path)
        .allMatch(path -> path.startsWith(TABLE_LOCATION));

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      FileScanTask task = only(tasks);
      assertThat(task.file().location()).isEqualTo(dataLocation);
      DeleteFile plannedDV = only(task.deletes());
      assertThat(plannedDV.location()).startsWith(TABLE_LOCATION);
      assertThat(plannedDV.referencedDataFile()).isEqualTo(dataLocation);
    }

    assertThat(Files.exists(temp.resolve("test-table/data/data.parquet"))).isTrue();
    assertThat(physicalFiles()).anyMatch(path -> path.toString().endsWith(".puffin"));

    assertThat(io.listPrefix(TABLE_LOCATION + "/"))
        .extracting(FileInfo::location)
        .allMatch(location -> location.startsWith(TABLE_LOCATION));

    table.expireSnapshots().expireOlderThan(Long.MAX_VALUE).retainLast(1).commit();
    assertNoPhysicalAliasLeakage();
    assertThat(io.metrics().bulkDeleteCount()).isGreaterThan(0L);
  }

  private CanonicalUriFileIO newFileIO() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(PathAliasProperties.DELEGATE_IMPL, HadoopFileIO.class.getName());
    properties.put(PathAliasProperties.UNMATCHED, "fail");
    properties.put("pathalias.rule.0.id", "test-table");
    properties.put("pathalias.rule.0.canonical", "s3://warehouse/");
    properties.put("pathalias.rule.0.physical", ensureTrailingSlash(temp.toUri().toString()));

    CanonicalUriFileIO io = new CanonicalUriFileIO();
    io.setConf(new Configuration());
    io.initialize(properties);
    return io;
  }

  private void assertNoPhysicalAliasLeakage() throws IOException {
    String physicalPrefix = temp.toUri().toString();
    for (Path path : physicalFiles()) {
      String bytes = new String(Files.readAllBytes(path), ISO_8859_1);
      assertThat(bytes).doesNotContain(physicalPrefix).doesNotContain(temp.toString());
    }
  }

  private List<Path> physicalFiles() throws IOException {
    try (Stream<Path> paths = Files.walk(temp)) {
      return paths.filter(Files::isRegularFile).collect(Collectors.toList());
    }
  }

  private static String ensureTrailingSlash(String location) {
    return location.endsWith("/") ? location : location + "/";
  }

  private static <T> T only(Iterable<T> values) {
    Iterator<T> iterator = values.iterator();
    T value = iterator.next();
    assertThat(iterator.hasNext()).isFalse();
    return value;
  }

  private static class InMemoryCatalogTableOperations extends BaseMetastoreTableOperations {
    private final FileIO io;
    private final AtomicReference<String> metadataLocation = new AtomicReference<>();

    private InMemoryCatalogTableOperations(FileIO io) {
      this.io = io;
    }

    @Override
    protected String tableName() {
      return "path-alias-v3";
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    protected void doRefresh() {
      String location = metadataLocation.get();
      if (location == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(location);
      }
    }

    @Override
    protected void doCommit(TableMetadata base, TableMetadata updated) {
      String newMetadataLocation = writeNewMetadataIfRequired(base == null, updated);
      String expectedLocation = base == null ? null : currentMetadataLocation();
      if (!metadataLocation.compareAndSet(expectedLocation, newMetadataLocation)) {
        io.deleteFile(newMetadataLocation);
        throw new CommitFailedException("Injected catalog pointer conflict");
      }
    }
  }
}
