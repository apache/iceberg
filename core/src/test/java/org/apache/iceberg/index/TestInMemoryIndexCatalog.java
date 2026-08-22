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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ConcurrentModificationException;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestInMemoryIndexCatalog {

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "orders");
  private static final IndexIdentifier IDX =
      IndexIdentifier.of(TABLE, "order_id_idx");

  private InMemoryIndexCatalog catalog;

  @BeforeEach
  void setup() {
    catalog = new InMemoryIndexCatalog();
  }

  private IndexMetadata sampleMetadata(String metadataLocation) {
    return GenericIndexMetadata.builder()
        .uuid("9c12d441-03fe-4693-9a96-a0705ddf69c1")
        .tableUuid("fb072c92-a02b-11e9-ae9c-1bb7bc9eca94")
        .location("s3://warehouse/db/orders/index/order_id_idx")
        .type("SCALAR")
        .transformFunction("HASH")
        .keyColumnIds(ImmutableList.of(3))
        .metadataFileLocation(metadataLocation)
        .build();
  }

  @Test
  void createAndLoad() {
    IndexMetadata metadata = sampleMetadata("s3://warehouse/.../metadata/00001-abc.metadata.json");
    catalog.createIndex(IDX, metadata);

    IndexMetadata loaded = catalog.loadIndex(IDX);
    assertThat(loaded.uuid()).isEqualTo(metadata.uuid());
    assertThat(loaded.type()).isEqualTo("SCALAR");
    assertThat(loaded.metadataFileLocation()).isEqualTo(metadata.metadataFileLocation());
  }

  @Test
  void createDuplicateThrows() {
    catalog.createIndex(IDX, sampleMetadata("s3://.../00001.metadata.json"));
    assertThatThrownBy(
            () -> catalog.createIndex(IDX, sampleMetadata("s3://.../00001.metadata.json")))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void loadNonExistentThrows() {
    assertThatThrownBy(() -> catalog.loadIndex(IDX))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("does not exist");
  }

  @Test
  void updateSucceeds() {
    IndexMetadata v1 = sampleMetadata("s3://.../00001.metadata.json");
    catalog.createIndex(IDX, v1);

    IndexMetadata v2 =
        GenericIndexMetadata.buildFrom(v1)
            .metadataFileLocation("s3://.../00002.metadata.json")
            .build();
    catalog.updateIndex(IDX, v1, v2);

    assertThat(catalog.loadIndex(IDX).metadataFileLocation())
        .isEqualTo("s3://.../00002.metadata.json");
  }

  @Test
  void updateConflictThrows() {
    IndexMetadata v1 = sampleMetadata("s3://.../00001.metadata.json");
    catalog.createIndex(IDX, v1);

    // Simulate another writer already committed v2
    IndexMetadata v2 =
        GenericIndexMetadata.buildFrom(v1)
            .metadataFileLocation("s3://.../00002.metadata.json")
            .build();
    catalog.updateIndex(IDX, v1, v2);

    // Now our writer tries to commit based on stale v1
    IndexMetadata v2conflict =
        GenericIndexMetadata.buildFrom(v1)
            .metadataFileLocation("s3://.../00002-conflict.metadata.json")
            .build();
    assertThatThrownBy(() -> catalog.updateIndex(IDX, v1, v2conflict))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("metadata location has changed");
  }

  @Test
  void dropAndExists() {
    catalog.createIndex(IDX, sampleMetadata("s3://.../00001.metadata.json"));
    assertThat(catalog.indexExists(IDX)).isTrue();

    catalog.dropIndex(IDX);
    assertThat(catalog.indexExists(IDX)).isFalse();
    assertThatThrownBy(() -> catalog.loadIndex(IDX))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  void listIndexes() {
    TableIdentifier table2 = TableIdentifier.of(Namespace.of("db"), "items");
    IndexIdentifier idx2 = IndexIdentifier.of(TABLE, "created_at_idx");
    IndexIdentifier idx3 = IndexIdentifier.of(table2, "item_id_idx");

    catalog.createIndex(IDX, sampleMetadata("s3://.../orders/00001.metadata.json"));
    catalog.createIndex(idx2, sampleMetadata("s3://.../created_at/00001.metadata.json"));
    catalog.createIndex(idx3, sampleMetadata("s3://.../items/00001.metadata.json"));

    List<IndexMetadata> ordersIndexes = catalog.listIndexes(TABLE);
    assertThat(ordersIndexes).hasSize(2);

    List<IndexMetadata> itemsIndexes = catalog.listIndexes(table2);
    assertThat(itemsIndexes).hasSize(1);
  }

  @Test
  void newMetadataFileLocationFormat() {
    String location = "s3://warehouse/db/orders/index/order_id_idx";
    String path = IndexMetadataIO.newMetadataFileLocation(location, null);
    assertThat(path).startsWith(location + "/metadata/00001-");
    assertThat(path).endsWith(".metadata.json");

    // Second write with existing metadata with 1 snapshot gets version 2
    IndexMetadata withOneSnapshot =
        GenericIndexMetadata.buildFrom(sampleMetadata("s3://x"))
            .addSnapshot(
                GenericIndexSnapshot.builder()
                    .snapshotId(1L)
                    .sourceTableSnapshotId(100L)
                    .timestampMs(System.currentTimeMillis())
                    .trackingFile("s3://x/tracking.parquet")
                    .build())
            .build();
    String path2 = IndexMetadataIO.newMetadataFileLocation(location, withOneSnapshot);
    assertThat(path2).startsWith(location + "/metadata/00002-");
  }
}
