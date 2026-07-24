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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOrphanFileUtils {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

  private File tableDir;
  private HadoopTables tables;

  @BeforeEach
  public void before() {
    tableDir =
        new File(System.getProperty("java.io.tmpdir"), "orphan-conflict-" + System.nanoTime());
    tables = new HadoopTables(new Configuration());
  }

  @AfterEach
  public void cleanup() {
    try {
      tables.dropTable(tableDir.getAbsolutePath());
    } catch (Exception ignored) {
      // best-effort cleanup
    }
  }

  @Test
  public void hasOtherTableInLocationReturnsTrueWhenForeignMetadataJsonPresent() {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    // A metadata.json belonging to a different table (different, randomly generated table-uuid)
    // written into this table's metadata directory must be detected as a conflict.
    TableMetadata foreign =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), tableDir.getAbsolutePath(), Maps.newHashMap());
    OutputFile foreignFile =
        table
            .io()
            .newOutputFile(tableDir.getAbsolutePath() + "/metadata/00002-foreign.metadata.json");
    TableMetadataParser.write(foreign, foreignFile);

    assertThat(OrphanFileUtils.hasOtherTableInLocation(table)).isTrue();
  }

  @Test
  public void hasOtherTableInLocationReturnsFalseForSingleTable() {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    assertThat(OrphanFileUtils.hasOtherTableInLocation(table)).isFalse();
  }

  @Test
  public void hasOtherTableInLocationIgnoresOldVersionsOfSameTable() {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    // An extra metadata.json that carries the SAME table-uuid (e.g. a truncated old version)
    // must NOT be treated as a conflict.
    String currentLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    TableMetadata current = TableMetadataParser.read(table.io(), currentLocation);
    OutputFile sameUuidFile =
        table
            .io()
            .newOutputFile(tableDir.getAbsolutePath() + "/metadata/00002-sameuuid.metadata.json");
    TableMetadataParser.write(current, sameUuidFile);

    assertThat(OrphanFileUtils.hasOtherTableInLocation(table)).isFalse();
  }

  @Test
  public void hasOtherTableInLocationReturnsTrueWhenForeignMetadataHasNoUuid() throws IOException {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    // Write a foreign metadata.json (its own random table-uuid), then strip the "uuid" field to
    // simulate a legacy file without a uuid. readTableUuid must return null and the location must
    // be treated as conflicting (fail-safe).
    TableMetadata foreign =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), tableDir.getAbsolutePath(), Maps.newHashMap());
    String foreignPath = tableDir.getAbsolutePath() + "/metadata/00002-foreign.metadata.json";
    TableMetadataParser.write(foreign, table.io().newOutputFile(foreignPath));

    String text =
        readMetadataText(table, foreignPath).replaceFirst("\"uuid\"\\s*:\\s*\"[^\"]*\"\\s*,?", "");
    writeMetadataBytes(table, foreignPath, text.getBytes(StandardCharsets.UTF_8));

    assertThat(OrphanFileUtils.hasOtherTableInLocation(table)).isTrue();
  }

  @Test
  public void hasOtherTableInLocationReturnsTrueForCompressedForeignMetadata() throws IOException {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    // A gzip-compressed metadata.json belonging to another table must also be detected.
    TableMetadata foreign =
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), tableDir.getAbsolutePath(), Maps.newHashMap());
    String tmpPath = tableDir.getAbsolutePath() + "/metadata/00002-foreign.metadata.json";
    TableMetadataParser.write(foreign, table.io().newOutputFile(tmpPath));
    byte[] jsonBytes = readMetadataText(table, tmpPath).getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(baos)) {
      gz.write(jsonBytes);
    }
    String gzPath = tableDir.getAbsolutePath() + "/metadata/00002-foreign.metadata.json.gz";
    writeMetadataBytes(table, gzPath, baos.toByteArray());

    assertThat(OrphanFileUtils.hasOtherTableInLocation(table)).isTrue();
  }

  @Test
  public void hasOtherTableInLocationReturnsTrueForUnreadableForeignMetadata() throws IOException {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    // A file that looks like a metadata.json but is not valid JSON must be treated as a conflict
    // (fail-safe), never silently ignored.
    String corruptPath = tableDir.getAbsolutePath() + "/metadata/00002-corrupt.metadata.json";
    writeMetadataBytes(
        table, corruptPath, "this is not valid json".getBytes(StandardCharsets.UTF_8));

    assertThat(OrphanFileUtils.hasOtherTableInLocation(table)).isTrue();
  }

  private String readMetadataText(Table table, String location) throws IOException {
    try (InputStream is = table.io().newInputFile(location).newStream()) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private void writeMetadataBytes(Table table, String location, byte[] bytes) throws IOException {
    try (PositionOutputStream os = table.io().newOutputFile(location).createOrOverwrite()) {
      os.write(bytes);
    }
  }

  @Test
  public void hasOtherTableInLocationThrowsWhenFileIoLacksPrefixSupport() {
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            Maps.newHashMap(),
            tableDir.getAbsolutePath());

    // A FileIO that does NOT implement SupportsPrefixOperations cannot run the location-conflict
    // check; the util must surface this as a ValidationException (so an ERROR-mode caller aborts)
    // rather than silently skipping or deleting files.
    Table wrapped = new DelegatingTable(table, new PlainFileIO());
    assertThatThrownBy(() -> OrphanFileUtils.hasOtherTableInLocation(wrapped))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot detect location conflicts");
  }

  /** A {@link Table} wrapper that overrides {@link Table#io()} but delegates everything else. */
  private static class DelegatingTable implements Table, HasTableOperations {
    private final Table delegate;
    private final FileIO io;

    DelegatingTable(Table delegate, FileIO io) {
      this.delegate = delegate;
      this.io = io;
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    public TableOperations operations() {
      return ((HasTableOperations) delegate).operations();
    }

    @Override
    public void refresh() {
      delegate.refresh();
    }

    @Override
    public TableScan newScan() {
      return delegate.newScan();
    }

    @Override
    public Schema schema() {
      return delegate.schema();
    }

    @Override
    public Map<Integer, Schema> schemas() {
      return delegate.schemas();
    }

    @Override
    public PartitionSpec spec() {
      return delegate.spec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
      return delegate.specs();
    }

    @Override
    public SortOrder sortOrder() {
      return delegate.sortOrder();
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
      return delegate.sortOrders();
    }

    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public Snapshot currentSnapshot() {
      return delegate.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
      return delegate.snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
      return delegate.snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
      return delegate.history();
    }

    @Override
    public UpdateSchema updateSchema() {
      return delegate.updateSchema();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
      return delegate.updateSpec();
    }

    @Override
    public UpdateProperties updateProperties() {
      return delegate.updateProperties();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
      return delegate.replaceSortOrder();
    }

    @Override
    public UpdateLocation updateLocation() {
      return delegate.updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return delegate.newAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return delegate.newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return delegate.rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return delegate.newOverwrite();
    }

    @Override
    public RowDelta newRowDelta() {
      return delegate.newRowDelta();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return delegate.newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return delegate.newDelete();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return delegate.expireSnapshots();
    }

    @Override
    public ManageSnapshots manageSnapshots() {
      return delegate.manageSnapshots();
    }

    @Override
    public Transaction newTransaction() {
      return delegate.newTransaction();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
      return delegate.statisticsFiles();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
      return delegate.refs();
    }
  }

  /**
   * A plain {@link FileIO} that deliberately does NOT implement {@link SupportsPrefixOperations}.
   */
  private static class PlainFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }
}
