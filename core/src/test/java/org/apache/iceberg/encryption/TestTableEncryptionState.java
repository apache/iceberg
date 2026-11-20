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
package org.apache.iceberg.encryption;

import static org.apache.iceberg.encryption.UnitestKMS.MASTER_KEY_NAME1;
import static org.apache.iceberg.encryption.UnitestKMS.MASTER_KEY_NAME2;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTableEncryptionState {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final int DEK_LENGTH = TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;
  private static final FileIO IO = new InMemoryFileIO();

  private TableEncryptionState state;

  @BeforeEach
  void before() {
    KeyManagementClient kmsClient = new UnitestKMS();
    kmsClient.initialize(Map.of());
    this.state = new TableEncryptionState(kmsClient);
  }

  @Test
  void unencryptedTable() {
    state.update(null, DEK_LENGTH, metadata(null));

    assertThat(state.encryption()).isSameAs(PlaintextEncryptionManager.instance());
    assertThat(state.io(IO)).isSameAs(IO);
  }

  @Test
  void encryptedTable() {
    EncryptedKey key =
        new BaseEncryptedKey("key-1", ByteBuffer.wrap(new byte[] {1}), MASTER_KEY_NAME1, Map.of());
    TableMetadata metadata =
        TableMetadata.buildFrom(metadata(MASTER_KEY_NAME1)).addEncryptionKey(key).build();
    int dekLength = 32;

    state.update(MASTER_KEY_NAME1, dekLength, metadata);

    EncryptionManager encryption = state.encryption();
    assertThat(encryption).isInstanceOf(StandardEncryptionManager.class);
    assertThat(keyIds(encryption)).containsExactly("key-1");
    NativeEncryptionKeyMetadata keyMetadata =
        (NativeEncryptionKeyMetadata) encryption.encrypt(IO.newOutputFile("file")).keyMetadata();
    assertThat(keyMetadata.encryptionKey().remaining()).isEqualTo(dekLength);

    FileIO io = state.io(IO);
    assertThat(io).isInstanceOf(EncryptingFileIO.class);
    assertThat(((EncryptingFileIO) io).encryptionManager()).isSameAs(encryption);
    assertThat(state.encryption()).isSameAs(encryption);
    assertThat(state.io(IO)).isSameAs(io);
  }

  @Test
  void updateKeepsKeysCreatedByEncryptionManager() {
    TableMetadata metadata = metadata(MASTER_KEY_NAME1);
    state.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata);
    EncryptionManager encryption = state.encryption();
    FileIO io = state.io(IO);
    String keyId = ((StandardEncryptionManager) encryption).keyEncryptionKeyID();

    state.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata);

    assertThat(state.encryption()).isNotSameAs(encryption);
    assertThat(state.io(IO)).isNotSameAs(io);
    assertThat(keyIds(state.encryption())).contains(keyId);
  }

  @Test
  void repeatedUpdatesKeepKeysCreatedByEncryptionManager() {
    TableMetadata metadata = metadata(MASTER_KEY_NAME1);
    state.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata);
    String keyId = ((StandardEncryptionManager) state.encryption()).keyEncryptionKeyID();

    state.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata);
    state.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata);

    assertThat(keyIds(state.encryption())).contains(keyId);
  }

  @Test
  void newTable() {
    state.adoptForNewTable(metadata(MASTER_KEY_NAME1));

    assertThat(state.encryption()).isInstanceOf(StandardEncryptionManager.class);
    assertThat(state.io(IO)).isInstanceOf(EncryptingFileIO.class);
  }

  @Test
  void newTableKeepsAdoptedKeyId() {
    state.adoptForNewTable(metadata(MASTER_KEY_NAME1));
    EncryptionManager encryption = state.encryption();

    state.adoptForNewTable(metadata(MASTER_KEY_NAME2));

    assertThat(state.encryption()).isSameAs(encryption);
  }

  @Test
  void committedNewTableKeepsKeysCreatedByEncryptionManager() {
    TableMetadata metadata = metadata(MASTER_KEY_NAME1);
    state.adoptForNewTable(metadata);
    String keyId = ((StandardEncryptionManager) state.encryption()).keyEncryptionKeyID();

    state.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata);

    assertThat(keyIds(state.encryption())).contains(keyId);
  }

  @Test
  void loadedTableIgnoresUncommittedMetadata() {
    state.update(null, DEK_LENGTH, metadata(null));

    state.adoptForNewTable(metadata(MASTER_KEY_NAME1));

    assertThat(state.encryption()).isSameAs(PlaintextEncryptionManager.instance());
    assertThat(state.io(IO)).isSameAs(IO);
  }

  @Test
  void missingKeyManagementClient() {
    TableEncryptionState unencrypted = new TableEncryptionState(null);
    unencrypted.update(null, DEK_LENGTH, metadata(null));
    assertThat(unencrypted.io(IO)).isSameAs(IO);

    TableEncryptionState encrypted = new TableEncryptionState(null);
    encrypted.update(MASTER_KEY_NAME1, DEK_LENGTH, metadata(MASTER_KEY_NAME1));
    String expectedMessage =
        "Cannot create encryption manager without a key management client. "
            + "Consider setting the 'encryption.kms-impl' catalog property";
    assertThatThrownBy(encrypted::encryption)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
    assertThatThrownBy(() -> encrypted.io(IO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  private static TableMetadata metadata(String tableKeyId) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.FORMAT_VERSION, "3");
    if (tableKeyId != null) {
      properties.put(TableProperties.ENCRYPTION_TABLE_KEY, tableKeyId);
    }

    return TableMetadata.newTableMetadata(
        SCHEMA, PartitionSpec.unpartitioned(), "file:/tmp/table", properties);
  }

  private static Set<String> keyIds(EncryptionManager encryption) {
    return EncryptionUtil.encryptionKeys(encryption).keySet();
  }
}
