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

import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.SortDirection.ASC;
import static org.apache.iceberg.TableProperties.CURRENT_SCHEMA;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_SUMMARY;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_TIMESTAMP;
import static org.apache.iceberg.TableProperties.DEFAULT_PARTITION_SPEC;
import static org.apache.iceberg.TableProperties.DEFAULT_SORT_ORDER;
import static org.apache.iceberg.TableProperties.SNAPSHOT_COUNT;
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Run all the tests from abstract of {@link CatalogTests} with few specific tests related to HIVE.
 */
public class TestHiveCatalog extends CatalogTests<HiveCatalog> {
  private static final ImmutableMap META =
      ImmutableMap.of(
          "owner", "apache",
          "group", "iceberg",
          "comment", "iceberg  hiveCatalog test");

  @TempDir private Path temp;

  private HiveCatalog catalog;
  private static final String DB_NAME = "hivedb";

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().build();

  @BeforeEach
  public void before() throws TException {
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());
    String dbPath = HIVE_METASTORE_EXTENSION.metastore().getDatabasePath(DB_NAME);
    Database db = new Database(DB_NAME, "description", dbPath, Maps.newHashMap());
    HIVE_METASTORE_EXTENSION.metastoreClient().createDatabase(db);
  }

  @AfterEach
  public void cleanup() throws Exception {
    HIVE_METASTORE_EXTENSION.metastore().reset();
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    return false;
  }

  @Override
  protected HiveCatalog catalog() {
    return catalog;
  }

  private Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
  }

  @Test
  public void testInvalidIdentifiersWithRename() {
    TableIdentifier invalidFrom = TableIdentifier.of(Namespace.of("l1", "l2"), "table1");
    TableIdentifier validTo = TableIdentifier.of(Namespace.of("l1"), "renamedTable");
    assertThatThrownBy(() -> catalog.renameTable(invalidFrom, validTo))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid identifier: " + invalidFrom);

    TableIdentifier validFrom = TableIdentifier.of(Namespace.of("l1"), "table1");
    TableIdentifier invalidTo = TableIdentifier.of(Namespace.of("l1", "l2"), "renamedTable");
    assertThatThrownBy(() -> catalog.renameTable(validFrom, invalidTo))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid identifier: " + invalidTo);
  }

  @Test
  public void testCreateTableBuilder() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.resolve("tbl").toString();

    try {
      Table table =
          catalog
              .buildTable(tableIdent, schema)
              .withPartitionSpec(spec)
              .withLocation(location)
              .withProperty("key1", "value1")
              .withProperty("key2", "value2")
              .create();

      assertThat(table.location()).isEqualTo(location);
      assertThat(table.schema().columns()).hasSize(2);
      assertThat(table.spec().fields()).hasSize(1);
      assertThat(table.properties()).containsEntry("key1", "value1");
      assertThat(table.properties()).containsEntry("key2", "value2");
      // default Parquet compression is explicitly set for new tables
      assertThat(table.properties())
          .containsEntry(
              TableProperties.PARQUET_COMPRESSION,
              TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableWithCaching() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.resolve("tbl").toString();
    ImmutableMap<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    Catalog cachingCatalog = CachingCatalog.wrap(catalog);

    try {
      Table table = cachingCatalog.createTable(tableIdent, schema, spec, location, properties);

      assertThat(table.location()).isEqualTo(location);
      assertThat(table.schema().columns()).hasSize(2);
      assertThat(table.spec().fields()).hasSize(1);
      assertThat(table.properties()).containsEntry("key1", "value1");
      assertThat(table.properties()).containsEntry("key2", "value2");
      // default Parquet compression is explicitly set for new tables
      assertThat(table.properties())
          .containsEntry(
              TableProperties.PARQUET_COMPRESSION,
              TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
    } finally {
      cachingCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testInitialize() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              HiveCatalog hiveCatalog = new HiveCatalog();
              hiveCatalog.initialize("hive", Maps.newHashMap());
            });
  }

  @Test
  public void testToStringWithoutSetConf() {
    assertThatNoException()
        .isThrownBy(
            () -> {
              HiveCatalog hiveCatalog = new HiveCatalog();
              hiveCatalog.toString();
            });
  }

  @Test
  public void testInitializeCatalogWithProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("uri", "thrift://examplehost:9083");
    properties.put("warehouse", "/user/hive/testwarehouse");
    HiveCatalog hiveCatalog = new HiveCatalog();
    hiveCatalog.initialize("hive", properties);

    assertThat(hiveCatalog.getConf().get("hive.metastore.uris"))
        .isEqualTo("thrift://examplehost:9083");
    assertThat(hiveCatalog.getConf().get("hive.metastore.warehouse.dir"))
        .isEqualTo("/user/hive/testwarehouse");
  }

  @Test
  public void testCreateTableTxnBuilder() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.resolve("tbl").toString();

    try {
      Transaction txn =
          catalog.buildTable(tableIdent, schema).withLocation(location).createTransaction();
      txn.commitTransaction();
      Table table = catalog.loadTable(tableIdent);

      assertThat(table.location()).isEqualTo(location);
      assertThat(table.schema().columns()).hasSize(2);
      assertThat(table.spec().isUnpartitioned()).isTrue();
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testReplaceTxnBuilder(int formatVersion) {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.resolve("tbl").toString();

    try {
      Transaction createTxn =
          catalog
              .buildTable(tableIdent, schema)
              .withPartitionSpec(spec)
              .withLocation(location)
              .withProperty("key1", "value1")
              .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
              .createOrReplaceTransaction();
      createTxn.commitTransaction();

      Table table = catalog.loadTable(tableIdent);
      assertThat(table.spec().fields()).hasSize(1);

      String newLocation = temp.resolve("tbl-2").toString();

      Transaction replaceTxn =
          catalog
              .buildTable(tableIdent, schema)
              .withProperty("key2", "value2")
              .withLocation(newLocation)
              .replaceTransaction();
      replaceTxn.commitTransaction();

      table = catalog.loadTable(tableIdent);
      assertThat(table.location()).isEqualTo(newLocation);
      assertThat(table.currentSnapshot()).isNull();
      if (formatVersion == 1) {
        PartitionSpec v1Expected =
            PartitionSpec.builderFor(table.schema())
                .alwaysNull("data", "data_bucket")
                .withSpecId(1)
                .build();
        assertThat(table.spec())
            .as("Table should have a spec with one void field")
            .isEqualTo(v1Expected);
      } else {
        assertThat(table.spec().isUnpartitioned()).as("Table spec must be unpartitioned").isTrue();
      }

      assertThat(table.properties()).containsEntry("key1", "value1");
      assertThat(table.properties()).containsEntry("key2", "value2");
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableWithOwner() throws Exception {
    createTableAndVerifyOwner(
        DB_NAME,
        "tbl_specified_owner",
        ImmutableMap.of(HiveCatalog.HMS_TABLE_OWNER, "some_owner"),
        "some_owner");
    createTableAndVerifyOwner(
        DB_NAME,
        "tbl_default_owner",
        ImmutableMap.of(),
        UserGroupInformation.getCurrentUser().getShortUserName());
  }

  private void createTableAndVerifyOwner(
      String db, String tbl, Map<String, String> properties, String owner)
      throws IOException, TException {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(db, tbl);
    String location = temp.resolve(tbl).toString();
    try {
      Table table = catalog.createTable(tableIdent, schema, spec, location, properties);
      org.apache.hadoop.hive.metastore.api.Table hmsTable =
          HIVE_METASTORE_EXTENSION.metastoreClient().getTable(db, tbl);
      assertThat(hmsTable.getOwner()).isEqualTo(owner);
      Map<String, String> hmsTableParams = hmsTable.getParameters();
      assertThat(hmsTableParams).doesNotContainKey(HiveCatalog.HMS_TABLE_OWNER);
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableDefaultSortOrder() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.createTable(tableIdent, schema, spec);
      assertThat(table.sortOrder().orderId()).as("Order ID must match").isEqualTo(0);
      assertThat(table.sortOrder().isUnsorted()).as("Order must unsorted").isTrue();

      assertThat(hmsTableParameters())
          .as("Must not have default sort order in catalog")
          .doesNotContainKey(DEFAULT_SORT_ORDER);
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableCustomSortOrder() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    SortOrder order = SortOrder.builderFor(schema).asc("id", NULLS_FIRST).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table =
          catalog
              .buildTable(tableIdent, schema)
              .withPartitionSpec(spec)
              .withSortOrder(order)
              .create();
      SortOrder sortOrder = table.sortOrder();
      assertThat(sortOrder.orderId()).as("Order ID must match").isEqualTo(1);
      assertThat(sortOrder.fields()).as("Order must have 1 field").hasSize(1);
      assertThat(sortOrder.fields().get(0).direction()).as("Direction must match ").isEqualTo(ASC);
      assertThat(sortOrder.fields().get(0).nullOrder())
          .as("Null order must match ")
          .isEqualTo(NULLS_FIRST);
      Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
      assertThat(sortOrder.fields().get(0).transform())
          .as("Transform must match")
          .isEqualTo(transform);

      assertThat(hmsTableParameters())
          .containsEntry(DEFAULT_SORT_ORDER, SortOrderParser.toJson(table.sortOrder()));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testDatabaseAndNamespaceWithLocation() throws Exception {
    Namespace namespace1 = Namespace.of("noLocation");
    catalog.createNamespace(namespace1, META);
    Database database1 =
        HIVE_METASTORE_EXTENSION.metastoreClient().getDatabase(namespace1.toString());

    assertThat(database1.getParameters()).containsEntry("owner", "apache");
    assertThat(database1.getParameters()).containsEntry("group", "iceberg");

    assertThat(defaultUri(namespace1))
        .as("There no same location for db and namespace")
        .isEqualTo(database1.getLocationUri());

    assertThatThrownBy(() -> catalog.createNamespace(namespace1))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage(String.format("Namespace already exists: %s", namespace1));
    String hiveLocalDir = temp.toFile().toURI().toString();
    // remove the trailing slash of the URI
    hiveLocalDir = hiveLocalDir.substring(0, hiveLocalDir.length() - 1);
    ImmutableMap newMeta =
        ImmutableMap.<String, String>builder()
            .putAll(META)
            .put("location", hiveLocalDir)
            .buildOrThrow();
    Namespace namespace2 = Namespace.of("haveLocation");

    catalog.createNamespace(namespace2, newMeta);
    Database database2 =
        HIVE_METASTORE_EXTENSION.metastoreClient().getDatabase(namespace2.toString());
    assertThat(hiveLocalDir)
        .as("There no same location for db and namespace")
        .isEqualTo(database2.getLocationUri());
  }

  @Test
  public void testCreateNamespaceWithOwnership() throws Exception {
    createNamespaceAndVerifyOwnership(
        "default_ownership_1",
        ImmutableMap.of(),
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    createNamespaceAndVerifyOwnership(
        "default_ownership_2",
        ImmutableMap.of(
            "non_owner_prop1", "value1",
            "non_owner_prop2", "value2"),
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    createNamespaceAndVerifyOwnership(
        "individual_ownership_1",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "apache",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.USER.name()),
        "apache",
        PrincipalType.USER);

    createNamespaceAndVerifyOwnership(
        "individual_ownership_2",
        ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "someone"),
        "someone",
        PrincipalType.USER);

    createNamespaceAndVerifyOwnership(
        "group_ownership",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "iceberg",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        "iceberg",
        PrincipalType.GROUP);

    assertThatThrownBy(
            () ->
                createNamespaceAndVerifyOwnership(
                    "create_with_owner_type_alone",
                    ImmutableMap.of(HiveCatalog.HMS_DB_OWNER_TYPE, PrincipalType.USER.name()),
                    "no_post_create_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Create namespace setting %s without setting %s is not allowed",
                HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER));

    assertThatThrownBy(
            () ->
                createNamespaceAndVerifyOwnership(
                    "create_with_invalid_owner_type",
                    ImmutableMap.of(
                        HiveCatalog.HMS_DB_OWNER, "iceberg",
                        HiveCatalog.HMS_DB_OWNER_TYPE, "invalidOwnerType"),
                    "no_post_create_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No enum constant " + PrincipalType.class.getCanonicalName());
  }

  private void createNamespaceAndVerifyOwnership(
      String name, Map<String, String> prop, String expectedOwner, PrincipalType expectedOwnerType)
      throws TException {
    Namespace namespace = Namespace.of(name);

    catalog.createNamespace(namespace, prop);
    Database db = HIVE_METASTORE_EXTENSION.metastoreClient().getDatabase(namespace.toString());

    assertThat(db.getOwnerName()).isEqualTo(expectedOwner);
    assertThat(db.getOwnerType()).isEqualTo(expectedOwnerType);
  }

  @Test
  public void testLoadNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_load");

    catalog.createNamespace(namespace, META);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    assertThat(nameMata).containsEntry("owner", "apache");
    assertThat(nameMata).containsEntry("group", "iceberg");
    assertThat(catalog.convertToDatabase(namespace, META).getLocationUri())
        .as("There no same location for db and namespace")
        .isEqualTo(nameMata.get("location"));
  }

  @Test
  public void testNamespaceExists() throws TException {
    Namespace namespace = Namespace.of("dbname_exists");

    catalog.createNamespace(namespace, META);

    assertThat(catalog.namespaceExists(namespace)).as("Should true to namespace exist").isTrue();
    assertThat(catalog.namespaceExists(Namespace.of("db2", "db2", "ns2")))
        .as("Should false to namespace doesn't exist")
        .isFalse();
  }

  @Test
  public void testSetNamespaceOwnership() throws TException {
    setNamespaceOwnershipAndVerify(
        "set_individual_ownership_on_default_owner",
        ImmutableMap.of(),
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_individual_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.USER.name()),
        System.getProperty("user.name"),
        PrincipalType.USER,
        "some_individual_owner",
        PrincipalType.USER);

    setNamespaceOwnershipAndVerify(
        "set_group_ownership_on_default_owner",
        ImmutableMap.of(),
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        System.getProperty("user.name"),
        PrincipalType.USER,
        "some_group_owner",
        PrincipalType.GROUP);

    setNamespaceOwnershipAndVerify(
        "change_individual_to_group_ownership",
        ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_owner"),
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        "some_owner",
        PrincipalType.USER,
        "some_group_owner",
        PrincipalType.GROUP);

    setNamespaceOwnershipAndVerify(
        "change_group_to_individual_ownership",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_individual_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.USER.name()),
        "some_group_owner",
        PrincipalType.GROUP,
        "some_individual_owner",
        PrincipalType.USER);

    assertThatThrownBy(
            () ->
                setNamespaceOwnershipAndVerify(
                    "set_owner_without_setting_owner_type",
                    ImmutableMap.of(),
                    ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_individual_owner"),
                    System.getProperty("user.name"),
                    PrincipalType.USER,
                    "no_post_setting_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Setting %s and %s has to be performed together or not at all",
                HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER));

    assertThatThrownBy(
            () ->
                setNamespaceOwnershipAndVerify(
                    "set_owner_type_without_setting_owner",
                    ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_owner"),
                    ImmutableMap.of(HiveCatalog.HMS_DB_OWNER_TYPE, PrincipalType.GROUP.name()),
                    "some_owner",
                    PrincipalType.USER,
                    "no_post_setting_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Setting %s and %s has to be performed together or not at all",
                HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER));

    assertThatThrownBy(
            () ->
                setNamespaceOwnershipAndVerify(
                    "set_invalid_owner_type",
                    ImmutableMap.of(),
                    ImmutableMap.of(
                        HiveCatalog.HMS_DB_OWNER, "iceberg",
                        HiveCatalog.HMS_DB_OWNER_TYPE, "invalidOwnerType"),
                    System.getProperty("user.name"),
                    PrincipalType.USER,
                    "no_post_setting_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "No enum constant org.apache.hadoop.hive.metastore.api.PrincipalType.invalidOwnerType");
  }

  @Test
  public void testSetNamespaceOwnershipNoop() throws TException, IOException {
    setNamespaceOwnershipAndVerify(
        "set_ownership_noop_1",
        ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_individual_owner"),
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_individual_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.USER.name()),
        "some_individual_owner",
        PrincipalType.USER,
        "some_individual_owner",
        PrincipalType.USER);

    setNamespaceOwnershipAndVerify(
        "set_ownership_noop_2",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        "some_group_owner",
        PrincipalType.GROUP,
        "some_group_owner",
        PrincipalType.GROUP);

    setNamespaceOwnershipAndVerify(
        "set_ownership_noop_3",
        ImmutableMap.of(),
        ImmutableMap.of(),
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER,
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    setNamespaceOwnershipAndVerify(
        "set_ownership_noop_4",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        ImmutableMap.of("unrelated_prop_1", "value_1", "unrelated_prop_2", "value_2"),
        "some_group_owner",
        PrincipalType.GROUP,
        "some_group_owner",
        PrincipalType.GROUP);
  }

  private void setNamespaceOwnershipAndVerify(
      String name,
      Map<String, String> propToCreate,
      Map<String, String> propToSet,
      String expectedOwnerPostCreate,
      PrincipalType expectedOwnerTypePostCreate,
      String expectedOwnerPostSet,
      PrincipalType expectedOwnerTypePostSet)
      throws TException {
    createNamespaceAndVerifyOwnership(
        name, propToCreate, expectedOwnerPostCreate, expectedOwnerTypePostCreate);

    catalog.setProperties(Namespace.of(name), propToSet);
    Database database = HIVE_METASTORE_EXTENSION.metastoreClient().getDatabase(name);

    assertThat(database.getOwnerName()).isEqualTo(expectedOwnerPostSet);
    assertThat(database.getOwnerType()).isEqualTo(expectedOwnerTypePostSet);
  }

  @Test
  public void testRemoveNamespaceOwnership() throws TException, IOException {
    removeNamespaceOwnershipAndVerify(
        "remove_individual_ownership",
        ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_owner"),
        ImmutableSet.of(HiveCatalog.HMS_DB_OWNER, HiveCatalog.HMS_DB_OWNER_TYPE),
        "some_owner",
        PrincipalType.USER,
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_group_ownership",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        ImmutableSet.of(HiveCatalog.HMS_DB_OWNER, HiveCatalog.HMS_DB_OWNER_TYPE),
        "some_group_owner",
        PrincipalType.GROUP,
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_ownership_on_default_noop_1",
        ImmutableMap.of(),
        ImmutableSet.of(HiveCatalog.HMS_DB_OWNER, HiveCatalog.HMS_DB_OWNER_TYPE),
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER,
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_ownership_on_default_noop_2",
        ImmutableMap.of(),
        ImmutableSet.of(),
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER,
        UserGroupInformation.getCurrentUser().getShortUserName(),
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_ownership_noop_1",
        ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_owner"),
        ImmutableSet.of(),
        "some_owner",
        PrincipalType.USER,
        "some_owner",
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_ownership_noop_2",
        ImmutableMap.of(
            HiveCatalog.HMS_DB_OWNER,
            "some_group_owner",
            HiveCatalog.HMS_DB_OWNER_TYPE,
            PrincipalType.GROUP.name()),
        ImmutableSet.of(),
        "some_group_owner",
        PrincipalType.GROUP,
        "some_group_owner",
        PrincipalType.GROUP);

    assertThatThrownBy(
            () ->
                removeNamespaceOwnershipAndVerify(
                    "remove_owner_without_removing_owner_type",
                    ImmutableMap.of(
                        HiveCatalog.HMS_DB_OWNER,
                        "some_individual_owner",
                        HiveCatalog.HMS_DB_OWNER_TYPE,
                        PrincipalType.USER.name()),
                    ImmutableSet.of(HiveCatalog.HMS_DB_OWNER),
                    "some_individual_owner",
                    PrincipalType.USER,
                    "no_post_remove_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Removing %s and %s has to be performed together or not at all",
                HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER));

    assertThatThrownBy(
            () ->
                removeNamespaceOwnershipAndVerify(
                    "remove_owner_type_without_removing_owner",
                    ImmutableMap.of(
                        HiveCatalog.HMS_DB_OWNER,
                        "some_group_owner",
                        HiveCatalog.HMS_DB_OWNER_TYPE,
                        PrincipalType.GROUP.name()),
                    ImmutableSet.of(HiveCatalog.HMS_DB_OWNER_TYPE),
                    "some_group_owner",
                    PrincipalType.GROUP,
                    "no_post_remove_expectation_due_to_exception_thrown",
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Removing %s and %s has to be performed together or not at all",
                HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER));
  }

  private void removeNamespaceOwnershipAndVerify(
      String name,
      Map<String, String> propToCreate,
      Set<String> propToRemove,
      String expectedOwnerPostCreate,
      PrincipalType expectedOwnerTypePostCreate,
      String expectedOwnerPostRemove,
      PrincipalType expectedOwnerTypePostRemove)
      throws TException {
    createNamespaceAndVerifyOwnership(
        name, propToCreate, expectedOwnerPostCreate, expectedOwnerTypePostCreate);

    catalog.removeProperties(Namespace.of(name), propToRemove);

    Database database = HIVE_METASTORE_EXTENSION.metastoreClient().getDatabase(name);

    assertThat(database.getOwnerName()).isEqualTo(expectedOwnerPostRemove);
    assertThat(database.getOwnerType()).isEqualTo(expectedOwnerTypePostRemove);
  }

  @Test
  public void dropNamespace() {
    Namespace namespace = Namespace.of("dbname_drop");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    Schema schema = getTestSchema();

    catalog.createNamespace(namespace, META);
    catalog.createTable(identifier, schema);
    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    assertThat(nameMata).containsEntry("owner", "apache");
    assertThat(nameMata).containsEntry("group", "iceberg");

    assertThatThrownBy(() -> catalog.dropNamespace(namespace))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace dbname_drop is not empty. One or more tables exist.");
    assertThat(catalog.dropTable(identifier, true)).isTrue();
    assertThat(catalog.dropNamespace(namespace))
        .as("Should fail to drop namespace if it is not empty")
        .isTrue();
    assertThat(catalog.dropNamespace(Namespace.of("db.ns1")))
        .as("Should fail to drop when namespace doesn't exist")
        .isFalse();
    assertThatThrownBy(() -> catalog.loadNamespaceMetadata(namespace))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: dbname_drop");
  }

  @Test
  public void testDropTableWithoutMetadataFile() {
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "tbl");
    Schema tableSchema = getTestSchema();
    catalog.createTable(identifier, tableSchema);
    String metadataFileLocation = catalog.newTableOps(identifier).current().metadataFileLocation();
    TableOperations ops = catalog.newTableOps(identifier);
    ops.io().deleteFile(metadataFileLocation);
    assertThat(catalog.dropTable(identifier)).isTrue();
    assertThatThrownBy(() -> catalog.loadTable(identifier))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist:");
  }

  @Test
  public void testTableName() {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      catalog.buildTable(tableIdent, schema).withPartitionSpec(spec).create();

      Table table = catalog.loadTable(tableIdent);
      assertThat(table.name()).as("Name must match").isEqualTo("hive.hivedb.tbl");

      TableIdentifier snapshotsTableIdent = TableIdentifier.of(DB_NAME, "tbl", "snapshots");
      Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
      assertThat(snapshotsTable.name())
          .as("Name must match")
          .isEqualTo("hive.hivedb.tbl.snapshots");
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  private String defaultUri(Namespace namespace) throws TException {
    return HIVE_METASTORE_EXTENSION
            .metastoreClient()
            .getConfigValue("hive.metastore.warehouse.dir", "")
        + "/"
        + namespace.level(0)
        + ".db";
  }

  @Test
  public void testUUIDinTableProperties() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdentifier = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.resolve("tbl").toString();

    try {
      catalog.buildTable(tableIdentifier, schema).withLocation(location).create();

      assertThat(hmsTableParameters()).containsKey(TableProperties.UUID);
    } finally {
      catalog.dropTable(tableIdentifier);
    }
  }

  @Test
  public void testSnapshotStatsTableProperties() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdentifier = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.resolve("tbl").toString();

    try {
      catalog.buildTable(tableIdentifier, schema).withLocation(location).create();

      // check whether parameters are in expected state
      Map<String, String> parameters = hmsTableParameters();
      assertThat(parameters).containsEntry(SNAPSHOT_COUNT, "0");
      assertThat(parameters)
          .doesNotContainKey(CURRENT_SNAPSHOT_SUMMARY)
          .doesNotContainKey(CURRENT_SNAPSHOT_ID)
          .doesNotContainKey(CURRENT_SNAPSHOT_TIMESTAMP);

      // create a snapshot
      Table icebergTable = catalog.loadTable(tableIdentifier);
      String fileName = UUID.randomUUID().toString();
      DataFile file =
          DataFiles.builder(icebergTable.spec())
              .withPath(FileFormat.PARQUET.addExtension(fileName))
              .withRecordCount(2)
              .withFileSizeInBytes(0)
              .build();
      icebergTable.newFastAppend().appendFile(file).commit();

      // check whether parameters are in expected state
      parameters = hmsTableParameters();
      assertThat(parameters).containsEntry(SNAPSHOT_COUNT, "1");
      String summary =
          JsonUtil.mapper().writeValueAsString(icebergTable.currentSnapshot().summary());
      assertThat(parameters).containsEntry(CURRENT_SNAPSHOT_SUMMARY, summary);
      long snapshotId = icebergTable.currentSnapshot().snapshotId();
      assertThat(parameters).containsEntry(CURRENT_SNAPSHOT_ID, String.valueOf(snapshotId));
      assertThat(parameters)
          .containsEntry(
              CURRENT_SNAPSHOT_TIMESTAMP,
              String.valueOf(icebergTable.currentSnapshot().timestampMillis()));
    } finally {
      catalog.dropTable(tableIdentifier);
    }
  }

  @Test
  public void testSetSnapshotSummary() throws Exception {
    Configuration conf = new Configuration();
    conf.set("iceberg.hive.table-property-max-size", "4000");
    HiveTableOperations ops =
        new HiveTableOperations(conf, null, null, catalog.name(), DB_NAME, "tbl");
    Snapshot snapshot = mock(Snapshot.class);
    Map<String, String> summary = Maps.newHashMap();
    when(snapshot.summary()).thenReturn(summary);

    // create a snapshot summary whose json string size is less than the limit
    for (int i = 0; i < 100; i++) {
      summary.put(String.valueOf(i), "value");
    }
    assertThat(JsonUtil.mapper().writeValueAsString(summary).length()).isLessThan(4000);
    Map<String, String> parameters = Maps.newHashMap();
    ops.setSnapshotSummary(parameters, snapshot);
    assertThat(parameters).as("The snapshot summary must be in parameters").hasSize(1);

    // create a snapshot summary whose json string size exceeds the limit
    for (int i = 0; i < 1000; i++) {
      summary.put(String.valueOf(i), "value");
    }
    long summarySize = JsonUtil.mapper().writeValueAsString(summary).length();
    // the limit has been updated to 4000 instead of the default value(32672)
    assertThat(summarySize).isGreaterThan(4000).isLessThan(32672);
    parameters.remove(CURRENT_SNAPSHOT_SUMMARY);
    ops.setSnapshotSummary(parameters, snapshot);
    assertThat(parameters)
        .as("The snapshot summary must not be in parameters due to the size limit")
        .isEmpty();
  }

  @Test
  public void testNotExposeTableProperties() {
    Configuration conf = new Configuration();
    conf.set("iceberg.hive.table-property-max-size", "0");
    HiveTableOperations ops =
        new HiveTableOperations(conf, null, null, catalog.name(), DB_NAME, "tbl");
    TableMetadata metadata = mock(TableMetadata.class);
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(CURRENT_SNAPSHOT_SUMMARY, "summary");
    parameters.put(CURRENT_SNAPSHOT_ID, "snapshotId");
    parameters.put(CURRENT_SNAPSHOT_TIMESTAMP, "timestamp");
    parameters.put(CURRENT_SCHEMA, "schema");
    parameters.put(DEFAULT_PARTITION_SPEC, "partitionSpec");
    parameters.put(DEFAULT_SORT_ORDER, "sortOrder");

    ops.setSnapshotStats(metadata, parameters);
    assertThat(parameters)
        .doesNotContainKey(CURRENT_SNAPSHOT_SUMMARY)
        .doesNotContainKey(CURRENT_SNAPSHOT_ID)
        .doesNotContainKey(CURRENT_SNAPSHOT_TIMESTAMP);

    ops.setSchema(metadata.schema(), parameters);
    assertThat(parameters).doesNotContainKey(CURRENT_SCHEMA);

    ops.setPartitionSpec(metadata, parameters);
    assertThat(parameters).doesNotContainKey(DEFAULT_PARTITION_SPEC);

    ops.setSortOrder(metadata, parameters);
    assertThat(parameters).doesNotContainKey(DEFAULT_SORT_ORDER);
  }

  @Test
  public void testSetDefaultPartitionSpec() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema).create();
      assertThat(hmsTableParameters())
          .as("Must not have default partition spec")
          .doesNotContainKey(TableProperties.DEFAULT_PARTITION_SPEC);

      table.updateSpec().addField(bucket("data", 16)).commit();
      assertThat(hmsTableParameters())
          .containsEntry(
              TableProperties.DEFAULT_PARTITION_SPEC, PartitionSpecParser.toJson(table.spec()));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testSetCurrentSchema() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema).create();

      assertThat(hmsTableParameters())
          .containsEntry(CURRENT_SCHEMA, SchemaParser.toJson(table.schema()));

      // add many new fields to make the schema json string exceed the limit
      UpdateSchema updateSchema = table.updateSchema();
      for (int i = 0; i < 600; i++) {
        updateSchema.addColumn("new_col_" + i, Types.StringType.get());
      }
      updateSchema.commit();

      assertThat(SchemaParser.toJson(table.schema()).length()).isGreaterThan(32672);
      assertThat(hmsTableParameters()).doesNotContainKey(CURRENT_SCHEMA);
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  private Map<String, String> hmsTableParameters() throws TException {
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        HIVE_METASTORE_EXTENSION.metastoreClient().getTable(DB_NAME, "tbl");
    return hmsTable.getParameters();
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    HiveCatalog catalogWithSlash = new HiveCatalog();
    String wareHousePath = "s3://bucket/db/tbl";

    catalogWithSlash.initialize(
        "hive_catalog", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, wareHousePath + "/"));
    assertThat(catalogWithSlash.getConf().get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname))
        .as("Should have trailing slash stripped")
        .isEqualTo(wareHousePath);
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "table-default.key1", "catalog-default-key1",
            "table-default.key2", "catalog-default-key2",
            "table-default.key3", "catalog-default-key3",
            "table-override.key3", "catalog-override-key3",
            "table-override.key4", "catalog-override-key4");
    Catalog hiveCatalog =
        CatalogUtil.loadCatalog(
            HiveCatalog.class.getName(),
            CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
            catalogProps,
            HIVE_METASTORE_EXTENSION.hiveConf());

    try {
      Table table =
          hiveCatalog
              .buildTable(tableIdent, schema)
              .withProperty("key2", "table-key2")
              .withProperty("key3", "table-key3")
              .withProperty("key5", "table-key5")
              .create();

      assertThat(table.properties())
          .as("Table defaults set for the catalog must be added to the table properties.")
          .containsEntry("key1", "catalog-default-key1");
      assertThat(table.properties())
          .as("Table property must override table default properties set at catalog level.")
          .containsEntry("key2", "table-key2");
      assertThat(table.properties())
          .as(
              "Table property override set at catalog level must override table default"
                  + " properties set at catalog level and table property specified.")
          .containsEntry("key3", "catalog-override-key3");
      assertThat(table.properties())
          .as("Table override not in table props or defaults should be added to table properties")
          .containsEntry("key4", "catalog-override-key4");
      assertThat(table.properties())
          .as(
              "Table properties without any catalog level default or override should be added to table"
                  + " properties.")
          .containsEntry("key5", "table-key5");
    } finally {
      hiveCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testDatabaseLocationWithSlashInWarehouseDir() {
    Configuration conf = new Configuration();
    // With a trailing slash
    conf.set("hive.metastore.warehouse.dir", "s3://bucket/");

    HiveCatalog hiveCatalog = new HiveCatalog();
    hiveCatalog.setConf(conf);

    Database database = hiveCatalog.convertToDatabase(Namespace.of("database"), ImmutableMap.of());

    assertThat(database.getLocationUri()).isEqualTo("s3://bucket/database.db");
  }
}
