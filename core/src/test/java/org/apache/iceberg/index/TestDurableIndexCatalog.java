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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.ConcurrentModificationException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unlike {@link TestInMemoryIndexCatalog}, uses a real {@link FileIO} against a real temp
 * directory (not opaque {@code s3://} strings) since {@link DurableIndexCatalog} actually reads
 * and writes pointer files and index metadata, and the whole point being tested is that a fresh
 * instance -- standing in for a process restart, sharing no in-memory state with the one that
 * wrote -- can still find what was registered.
 */
public class TestDurableIndexCatalog {

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "orders");
  private static final IndexIdentifier IDX = IndexIdentifier.of(TABLE, "order_id_idx");
  private static final String TABLE_UUID = "fb072c92-a02b-11e9-ae9c-1bb7bc9eca94";

  @TempDir private File tableDir;

  private FileIO io;
  private String tableLocation;

  @BeforeEach
  void setup() {
    this.io = new HadoopFileIO(new Configuration());
    this.tableLocation = tableDir.toURI().toString();
  }

  private DurableIndexCatalog newCatalog() {
    return newCatalog(TABLE_UUID);
  }

  private DurableIndexCatalog newCatalog(String tableUuid) {
    return new DurableIndexCatalog(io, tableLocation, tableUuid);
  }

  private IndexMetadata sampleMetadata(String metadataLocation) {
    return GenericIndexMetadata.builder()
        .uuid("9c12d441-03fe-4693-9a96-a0705ddf69c1")
        .tableUuid(TABLE_UUID)
        .location(tableLocation + "index/order_id_idx")
        .type("SCALAR")
        .transformFunction("HASH")
        .keyColumnIds(ImmutableList.of(3))
        .metadataFileLocation(metadataLocation)
        .build();
  }

  /** Writes a real metadata JSON file so loadIndex() has something real to read back. */
  private String writeRealMetadataFile(String version) {
    String location = tableLocation + "index/order_id_idx/metadata/" + version + ".metadata.json";
    IndexMetadataIO.write(sampleMetadata(location), io.newOutputFile(location));
    return location;
  }

  @Test
  void createAndLoad() {
    String location = writeRealMetadataFile("00001");
    DurableIndexCatalog catalog = newCatalog();
    catalog.createIndex(IDX, sampleMetadata(location));

    IndexMetadata loaded = catalog.loadIndex(IDX);
    assertThat(loaded.type()).isEqualTo("SCALAR");
    assertThat(loaded.metadataFileLocation()).isEqualTo(location);
  }

  @Test
  void survivesFreshCatalogInstance() {
    // Stands in for a process restart: a brand-new DurableIndexCatalog, sharing no in-memory
    // state with the one that wrote, must still find the index via its durable pointer file.
    String location = writeRealMetadataFile("00001");
    newCatalog().createIndex(IDX, sampleMetadata(location));

    DurableIndexCatalog freshCatalog = newCatalog();
    assertThat(freshCatalog.indexExists(IDX)).isTrue();
    assertThat(freshCatalog.loadIndex(IDX).metadataFileLocation()).isEqualTo(location);
  }

  @Test
  void doesNotLeakRegistrationsAcrossTableUuidsAtTheSameLocation() {
    // Regression test: a table's location string can be reused after a drop and recreate (most
    // catalogs do not purge files this class doesn't know about), so scoping only by location
    // -- not also by table UUID -- would let a brand-new, logically unrelated table silently
    // inherit a previous table's index registrations at the same path.
    String location = writeRealMetadataFile("00001");
    newCatalog("11111111-1111-1111-1111-111111111111").createIndex(IDX, sampleMetadata(location));

    DurableIndexCatalog differentTable = newCatalog("22222222-2222-2222-2222-222222222222");
    assertThat(differentTable.indexExists(IDX)).isFalse();
    assertThatThrownBy(() -> differentTable.loadIndex(IDX))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  void createDuplicateThrows() {
    String location = writeRealMetadataFile("00001");
    DurableIndexCatalog catalog = newCatalog();
    catalog.createIndex(IDX, sampleMetadata(location));

    assertThatThrownBy(() -> catalog.createIndex(IDX, sampleMetadata(location)))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void loadNonExistentThrows() {
    assertThatThrownBy(() -> newCatalog().loadIndex(IDX))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("does not exist");
  }

  @Test
  void updateSucceedsAndPersistsAcrossFreshInstance() {
    String location1 = writeRealMetadataFile("00001");
    DurableIndexCatalog catalog = newCatalog();
    IndexMetadata v1 = sampleMetadata(location1);
    catalog.createIndex(IDX, v1);

    String location2 = writeRealMetadataFile("00002");
    IndexMetadata v2 = GenericIndexMetadata.buildFrom(v1).metadataFileLocation(location2).build();
    catalog.updateIndex(IDX, v1, v2);

    assertThat(newCatalog().loadIndex(IDX).metadataFileLocation()).isEqualTo(location2);
  }

  @Test
  void updateConflictThrows() {
    String location1 = writeRealMetadataFile("00001");
    DurableIndexCatalog catalog = newCatalog();
    IndexMetadata v1 = sampleMetadata(location1);
    catalog.createIndex(IDX, v1);

    String location2 = writeRealMetadataFile("00002");
    IndexMetadata v2 = GenericIndexMetadata.buildFrom(v1).metadataFileLocation(location2).build();
    catalog.updateIndex(IDX, v1, v2); // simulate another writer already committing v2

    String conflictLocation = writeRealMetadataFile("00002-conflict");
    IndexMetadata v2Conflict =
        GenericIndexMetadata.buildFrom(v1).metadataFileLocation(conflictLocation).build();
    assertThatThrownBy(() -> catalog.updateIndex(IDX, v1, v2Conflict))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("metadata location has changed");
  }

  @Test
  void dropAndExists() {
    String location = writeRealMetadataFile("00001");
    DurableIndexCatalog catalog = newCatalog();
    catalog.createIndex(IDX, sampleMetadata(location));
    assertThat(catalog.indexExists(IDX)).isTrue();

    catalog.dropIndex(IDX);
    assertThat(catalog.indexExists(IDX)).isFalse();
    assertThatThrownBy(() -> catalog.loadIndex(IDX)).isInstanceOf(NoSuchTableException.class);
  }

  @Test
  void dropNonExistentThrows() {
    assertThatThrownBy(() -> newCatalog().dropIndex(IDX))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  void listIndexes() {
    DurableIndexCatalog catalog = newCatalog();

    String location1 = writeRealMetadataFile("00001");
    catalog.createIndex(IDX, sampleMetadata(location1));

    IndexIdentifier idx2 = IndexIdentifier.of(TABLE, "created_at_idx");
    String location2 = tableLocation + "index/created_at_idx/metadata/00001.metadata.json";
    IndexMetadata meta2 =
        GenericIndexMetadata.builder()
            .uuid("a1b2c3d4-03fe-4693-9a96-a0705ddf69c2")
            .tableUuid("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94")
            .location(tableLocation + "index/created_at_idx")
            .type("SCALAR")
            .transformFunction("IDENTITY")
            .keyColumnIds(ImmutableList.of(5))
            .metadataFileLocation(location2)
            .build();
    IndexMetadataIO.write(meta2, io.newOutputFile(location2));
    catalog.createIndex(idx2, meta2);

    List<IndexMetadata> indexes = catalog.listIndexes(TABLE);
    assertThat(indexes).hasSize(2);
  }
}
