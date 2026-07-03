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
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.File;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestSnapshotProducerWithEncryption {

  @TempDir private File tableDir;

  @Test
  void propagateEncryptionKeysToMetadata() {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();

    EncryptedTableOps ops = new EncryptedTableOps("encryption_test", tableDir);
    TestTables.TestTable table =
        TestTables.create(tableDir, "encryption_test", schema, spec, SortOrder.unsorted(), 3, ops);

    table.updateProperties().set(TableProperties.ENCRYPTION_TABLE_KEY, "keyA").commit();

    DataFile dataFile1 =
        DataFiles.builder(spec)
            .withPath(table.location() + "/data/file1.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile1).commit();

    Snapshot snapshot1 = table.currentSnapshot();
    String keyId1 = snapshot1.keyId();
    assertThat(keyId1).isNotNull();

    TableMetadata metadata1 = ops.current();
    assertThat(metadata1.encryptionKeys())
        .hasSize(2)
        .extracting(EncryptedKey::keyId)
        .contains(keyId1);

    assertThatCode(() -> table.newScan().planFiles()).doesNotThrowAnyException();

    DataFile dataFile2 =
        DataFiles.builder(spec)
            .withPath(table.location() + "/data/file2.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile2).commit();

    Snapshot snapshot2 = table.currentSnapshot();
    String keyId2 = snapshot2.keyId();
    assertThat(keyId2).isNotNull();
    assertThat(keyId2).isNotEqualTo(keyId1);

    TableMetadata metadata2 = ops.current();
    assertThat(metadata2.encryptionKeys())
        .hasSize(3)
        .extracting(EncryptedKey::keyId)
        .contains(keyId1, keyId2);
  }

  static class EncryptedTableOps extends TestTables.TestTableOperations {
    private EncryptionManager encryptionManager;

    EncryptedTableOps(String tableName, File location) {
      super(tableName, location);
      updateEncryptionManager();
    }

    @Override
    public EncryptionManager encryption() {
      return encryptionManager;
    }

    @Override
    public FileIO io() {
      return EncryptingFileIO.combine(super.io(), encryptionManager);
    }

    @Override
    public TableMetadata refresh() {
      TableMetadata metadata = super.refresh();
      updateEncryptionManager();
      return metadata;
    }

    private void updateEncryptionManager() {
      TableMetadata metadata = current();
      if (metadata == null) {
        encryptionManager = PlaintextEncryptionManager.instance();
        return;
      }

      Map<String, String> properties = metadata.properties();
      String tableKeyId = properties.get(TableProperties.ENCRYPTION_TABLE_KEY);
      if (tableKeyId == null) {
        encryptionManager = PlaintextEncryptionManager.instance();
        return;
      }

      Map<String, String> catalogProps =
          ImmutableMap.of("encryption.kms-impl", "org.apache.iceberg.encryption.UnitestKMS");
      encryptionManager =
          EncryptionUtil.createEncryptionManager(
              metadata.encryptionKeys(), properties, EncryptionUtil.createKmsClient(catalogProps));
    }
  }
}
