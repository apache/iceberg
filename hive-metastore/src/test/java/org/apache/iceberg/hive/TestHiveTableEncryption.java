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
package org.apache.iceberg.hive;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

public class TestHiveTableEncryption {
  private static final String DB_NAME = "hivedb";
  private static final String TABLE_NAME = "encrypted_tbl";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().withDatabase(DB_NAME).build();

  @TempDir private Path temp;

  private HiveCatalog catalog;

  @BeforeEach
  public void before() {
    TrackingKMS.reset();
    this.catalog = initCatalog();
  }

  @AfterEach
  public void after() {
    catalog.dropTable(TABLE_ID);
  }

  @Test
  public void keyGeneratedInKmsForCreatedTable() {
    Table table = createTable(ImmutableMap.of(kmsKeyGenerationProperty(), "true"));

    createKeyEncryptionKey(table);

    assertThat(TrackingKMS.GENERATE_CALLS).hasValue(1);
    assertThat(TrackingKMS.WRAP_CALLS).hasValue(0);
  }

  @Test
  public void keyGeneratedInKmsForLoadedTable() {
    createTable(ImmutableMap.of(kmsKeyGenerationProperty(), "true"));

    Table loaded = initCatalog().loadTable(TABLE_ID);
    createKeyEncryptionKey(loaded);

    assertThat(TrackingKMS.GENERATE_CALLS).hasValue(1);
    assertThat(TrackingKMS.WRAP_CALLS).hasValue(0);
  }

  @Test
  public void keyWrappedByDefault() {
    createTable(ImmutableMap.of());

    Table loaded = initCatalog().loadTable(TABLE_ID);
    createKeyEncryptionKey(loaded);

    assertThat(TrackingKMS.GENERATE_CALLS).hasValue(0);
    assertThat(TrackingKMS.WRAP_CALLS).hasValue(1);
  }

  @Test
  public void hmsParameterIsUsedForLoadedTable() throws Exception {
    createTable(ImmutableMap.of());

    // change the HMS parameter only; the metadata file still has no such property
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        HIVE_METASTORE_EXTENSION.metastoreClient().getTable(DB_NAME, TABLE_NAME);
    hmsTable.getParameters().put(kmsKeyGenerationProperty(), "true");
    HIVE_METASTORE_EXTENSION.metastoreClient().alter_table(DB_NAME, TABLE_NAME, hmsTable);

    Table loaded = initCatalog().loadTable(TABLE_ID);
    createKeyEncryptionKey(loaded);

    assertThat(TrackingKMS.GENERATE_CALLS).hasValue(1);
    assertThat(TrackingKMS.WRAP_CALLS).hasValue(0);
  }

  private static String kmsKeyGenerationProperty() {
    return TableProperties.ENCRYPTION_KMS_KEY_GENERATION_ENABLED;
  }

  private static HiveCatalog initCatalog() {
    return (HiveCatalog)
        CatalogUtil.loadCatalog(
            HiveCatalog.class.getName(),
            "hive",
            ImmutableMap.of(CatalogProperties.ENCRYPTION_KMS_IMPL, TrackingKMS.class.getName()),
            HIVE_METASTORE_EXTENSION.hiveConf());
  }

  private Table createTable(Map<String, String> extraProperties) {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(TableProperties.FORMAT_VERSION, "3")
            .put(TableProperties.ENCRYPTION_TABLE_KEY, UnitestKMS.MASTER_KEY_NAME1)
            .putAll(extraProperties)
            .build();
    return catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), properties);
  }

  // registering file key metadata creates a key encryption key if there is no valid one
  private void createKeyEncryptionKey(Table table) {
    EncryptionManager em = ((HasTableOperations) table).operations().encryption();
    assertThat(em).isInstanceOf(StandardEncryptionManager.class);

    StandardEncryptionManager sem = (StandardEncryptionManager) em;
    NativeEncryptionOutputFile outputFile =
        sem.encrypt(Files.localOutput(temp.resolve("data.parquet").toFile()));

    TrackingKMS.reset();
    sem.registerKeyMetadata(outputFile.keyMetadata());
  }

  public static class TrackingKMS extends UnitestKMS {
    static final AtomicInteger WRAP_CALLS = new AtomicInteger();
    static final AtomicInteger GENERATE_CALLS = new AtomicInteger();

    static void reset() {
      WRAP_CALLS.set(0);
      GENERATE_CALLS.set(0);
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
      WRAP_CALLS.incrementAndGet();
      return super.wrapKey(key, wrappingKeyId);
    }

    @Override
    public boolean supportsKeyGeneration() {
      return true;
    }

    @Override
    public KeyManagementClient.KeyGenerationResult generateKey(String wrappingKeyId) {
      GENERATE_CALLS.incrementAndGet();
      byte[] bytes = new byte[16];
      new SecureRandom().nextBytes(bytes);
      ByteBuffer key = ByteBuffer.wrap(bytes);
      return new KeyManagementClient.KeyGenerationResult(key, super.wrapKey(key, wrappingKeyId));
    }
  }
}
