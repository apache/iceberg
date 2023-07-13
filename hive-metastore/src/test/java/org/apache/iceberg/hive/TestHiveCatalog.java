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
import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import org.apache.iceberg.HasTableOperations;
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
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHiveCatalog extends HiveMetastoreTest {
  private static ImmutableMap meta =
      ImmutableMap.of(
          "owner", "apache",
          "group", "iceberg",
          "comment", "iceberg  hiveCatalog test");

  @TempDir private Path temp;

  private Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
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

      Assertions.assertEquals(location, table.location());
      Assertions.assertEquals(2, table.schema().columns().size());
      Assertions.assertEquals(1, table.spec().fields().size());
      Assertions.assertEquals("value1", table.properties().get("key1"));
      Assertions.assertEquals("value2", table.properties().get("key2"));
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

      Assertions.assertEquals(location, table.location());
      Assertions.assertEquals(2, table.schema().columns().size());
      Assertions.assertEquals(1, table.spec().fields().size());
      Assertions.assertEquals("value1", table.properties().get("key1"));
      Assertions.assertEquals("value2", table.properties().get("key2"));
    } finally {
      cachingCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testInitialize() {
    Assertions.assertDoesNotThrow(
        () -> {
          HiveCatalog catalog = new HiveCatalog();
          catalog.initialize("hive", Maps.newHashMap());
        });
  }

  @Test
  public void testToStringWithoutSetConf() {
    Assertions.assertDoesNotThrow(
        () -> {
          HiveCatalog catalog = new HiveCatalog();
          catalog.toString();
        });
  }

  @Test
  public void testInitializeCatalogWithProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("uri", "thrift://examplehost:9083");
    properties.put("warehouse", "/user/hive/testwarehouse");
    HiveCatalog catalog = new HiveCatalog();
    catalog.initialize("hive", properties);

    Assertions.assertEquals(
        catalog.getConf().get("hive.metastore.uris"), "thrift://examplehost:9083");
    Assertions.assertEquals(
        catalog.getConf().get("hive.metastore.warehouse.dir"), "/user/hive/testwarehouse");
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

      Assertions.assertEquals(location, table.location());
      Assertions.assertEquals(2, table.schema().columns().size());
      Assertions.assertTrue(table.spec().isUnpartitioned());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testReplaceTxnBuilder() throws Exception {
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
              .createOrReplaceTransaction();
      createTxn.commitTransaction();

      Table table = catalog.loadTable(tableIdent);
      Assertions.assertEquals(1, table.spec().fields().size());

      String newLocation = temp.resolve("tbl-2").toString();

      Transaction replaceTxn =
          catalog
              .buildTable(tableIdent, schema)
              .withProperty("key2", "value2")
              .withLocation(newLocation)
              .replaceTransaction();
      replaceTxn.commitTransaction();

      table = catalog.loadTable(tableIdent);
      Assertions.assertEquals(newLocation, table.location());
      Assertions.assertNull(table.currentSnapshot());
      PartitionSpec v1Expected =
          PartitionSpec.builderFor(table.schema())
              .alwaysNull("data", "data_bucket")
              .withSpecId(1)
              .build();
      Assertions.assertEquals(
          v1Expected, table.spec(), "Table should have a spec with one void field");

      Assertions.assertEquals("value1", table.properties().get("key1"));
      Assertions.assertEquals("value2", table.properties().get("key2"));
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
      org.apache.hadoop.hive.metastore.api.Table hmsTable = metastoreClient.getTable(db, tbl);
      Assertions.assertEquals(owner, hmsTable.getOwner());
      Map<String, String> hmsTableParams = hmsTable.getParameters();
      Assertions.assertFalse(hmsTableParams.containsKey(HiveCatalog.HMS_TABLE_OWNER));
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
      Assertions.assertEquals(0, table.sortOrder().orderId(), "Order ID must match");
      Assertions.assertTrue(table.sortOrder().isUnsorted(), "Order must unsorted");

      Assertions.assertFalse(
          hmsTableParameters().containsKey(DEFAULT_SORT_ORDER),
          "Must not have default sort order in catalog");
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
      Assertions.assertEquals(1, sortOrder.orderId(), "Order ID must match");
      Assertions.assertEquals(1, sortOrder.fields().size(), "Order must have 1 field");
      Assertions.assertEquals(ASC, sortOrder.fields().get(0).direction(), "Direction must match ");
      Assertions.assertEquals(
          NULLS_FIRST, sortOrder.fields().get(0).nullOrder(), "Null order must match ");
      Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
      Assertions.assertEquals(
          transform, sortOrder.fields().get(0).transform(), "Transform must match");

      Assertions.assertEquals(
          SortOrderParser.toJson(table.sortOrder()), hmsTableParameters().get(DEFAULT_SORT_ORDER));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateNamespace() throws Exception {
    Namespace namespace1 = Namespace.of("noLocation");
    catalog.createNamespace(namespace1, meta);
    Database database1 = metastoreClient.getDatabase(namespace1.toString());

    Assertions.assertTrue(database1.getParameters().get("owner").equals("apache"));
    Assertions.assertTrue(database1.getParameters().get("group").equals("iceberg"));

    Assertions.assertEquals(
        database1.getLocationUri(),
        defaultUri(namespace1),
        "There no same location for db and namespace");

    assertThatThrownBy(() -> catalog.createNamespace(namespace1))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Namespace '" + namespace1 + "' already exists!");
    String hiveLocalDir = temp.toFile().toURI().toString();
    // remove the trailing slash of the URI
    hiveLocalDir = hiveLocalDir.substring(0, hiveLocalDir.length() - 1);
    ImmutableMap newMeta =
        ImmutableMap.<String, String>builder()
            .putAll(meta)
            .put("location", hiveLocalDir)
            .buildOrThrow();
    Namespace namespace2 = Namespace.of("haveLocation");

    catalog.createNamespace(namespace2, newMeta);
    Database database2 = metastoreClient.getDatabase(namespace2.toString());
    Assertions.assertEquals(
        database2.getLocationUri(), hiveLocalDir, "There no same location for db and namespace");
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
    Database db = metastoreClient.getDatabase(namespace.toString());

    Assertions.assertEquals(expectedOwner, db.getOwnerName());
    Assertions.assertEquals(expectedOwnerType, db.getOwnerType());
  }

  @Test
  public void testListNamespace() throws TException {
    List<Namespace> namespaces;
    Namespace namespace1 = Namespace.of("dbname1");
    catalog.createNamespace(namespace1, meta);
    namespaces = catalog.listNamespaces(namespace1);
    Assertions.assertTrue(namespaces.isEmpty(), "Hive db not hive the namespace 'dbname1'");

    Namespace namespace2 = Namespace.of("dbname2");
    catalog.createNamespace(namespace2, meta);
    namespaces = catalog.listNamespaces();

    Assertions.assertTrue(
        namespaces.contains(namespace2), "Hive db not hive the namespace 'dbname2'");
  }

  @Test
  public void testLoadNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_load");

    catalog.createNamespace(namespace, meta);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assertions.assertTrue(nameMata.get("owner").equals("apache"));
    Assertions.assertTrue(nameMata.get("group").equals("iceberg"));
    Assertions.assertEquals(
        nameMata.get("location"),
        catalog.convertToDatabase(namespace, meta).getLocationUri(),
        "There no same location for db and namespace");
  }

  @Test
  public void testNamespaceExists() throws TException {
    Namespace namespace = Namespace.of("dbname_exists");

    catalog.createNamespace(namespace, meta);

    Assertions.assertTrue(catalog.namespaceExists(namespace), "Should true to namespace exist");
    Assertions.assertTrue(
        !catalog.namespaceExists(Namespace.of("db2", "db2", "ns2")),
        "Should false to namespace doesn't exist");
  }

  @Test
  public void testSetNamespaceProperties() throws TException {
    Namespace namespace = Namespace.of("dbname_set");

    catalog.createNamespace(namespace, meta);
    catalog.setProperties(
        namespace,
        ImmutableMap.of(
            "owner", "alter_apache",
            "test", "test",
            "location", "file:/data/tmp",
            "comment", "iceberg test"));

    Database database = metastoreClient.getDatabase(namespace.level(0));
    Assertions.assertEquals(database.getParameters().get("owner"), "alter_apache");
    Assertions.assertEquals(database.getParameters().get("test"), "test");
    Assertions.assertEquals(database.getParameters().get("group"), "iceberg");

    assertThatThrownBy(
            () -> catalog.setProperties(Namespace.of("db2", "db2", "ns2"), ImmutableMap.of()))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db2.db2.ns2");
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
    Database database = metastoreClient.getDatabase(name);

    Assertions.assertEquals(expectedOwnerPostSet, database.getOwnerName());
    Assertions.assertEquals(expectedOwnerTypePostSet, database.getOwnerType());
  }

  @Test
  public void testRemoveNamespaceProperties() throws TException {
    Namespace namespace = Namespace.of("dbname_remove");

    catalog.createNamespace(namespace, meta);

    catalog.removeProperties(namespace, ImmutableSet.of("comment", "owner"));

    Database database = metastoreClient.getDatabase(namespace.level(0));

    Assertions.assertEquals(database.getParameters().get("owner"), null);
    Assertions.assertEquals(database.getParameters().get("group"), "iceberg");

    assertThatThrownBy(
            () ->
                catalog.removeProperties(
                    Namespace.of("db2", "db2", "ns2"), ImmutableSet.of("comment", "owner")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessage("Namespace does not exist: db2.db2.ns2");
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

    Database database = metastoreClient.getDatabase(name);

    Assertions.assertEquals(expectedOwnerPostRemove, database.getOwnerName());
    Assertions.assertEquals(expectedOwnerTypePostRemove, database.getOwnerType());
  }

  @Test
  public void testDropNamespace() throws TException {
    Namespace namespace = Namespace.of("dbname_drop");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    Schema schema = getTestSchema();

    catalog.createNamespace(namespace, meta);
    catalog.createTable(identifier, schema);
    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assertions.assertTrue(nameMata.get("owner").equals("apache"));
    Assertions.assertTrue(nameMata.get("group").equals("iceberg"));

    assertThatThrownBy(() -> catalog.dropNamespace(namespace))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Namespace dbname_drop is not empty. One or more tables exist.");
    Assertions.assertTrue(catalog.dropTable(identifier, true));
    Assertions.assertTrue(
        catalog.dropNamespace(namespace), "Should fail to drop namespace if it is not empty");
    Assertions.assertFalse(
        catalog.dropNamespace(Namespace.of("db.ns1")),
        "Should fail to drop when namespace doesn't exist");
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
    Assertions.assertTrue(catalog.dropTable(identifier));
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
      Assertions.assertEquals("hive.hivedb.tbl", table.name(), "Name must match");

      TableIdentifier snapshotsTableIdent = TableIdentifier.of(DB_NAME, "tbl", "snapshots");
      Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
      Assertions.assertEquals(
          "hive.hivedb.tbl.snapshots", snapshotsTable.name(), "Name must match");
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  private String defaultUri(Namespace namespace) throws TException {
    return metastoreClient.getConfigValue("hive.metastore.warehouse.dir", "")
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

      Assertions.assertNotNull(hmsTableParameters().get(TableProperties.UUID));
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
      Assertions.assertEquals("0", parameters.get(TableProperties.SNAPSHOT_COUNT));
      Assertions.assertNull(parameters.get(CURRENT_SNAPSHOT_SUMMARY));
      Assertions.assertNull(parameters.get(CURRENT_SNAPSHOT_ID));
      Assertions.assertNull(parameters.get(CURRENT_SNAPSHOT_TIMESTAMP));

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
      Assertions.assertEquals("1", parameters.get(TableProperties.SNAPSHOT_COUNT));
      String summary =
          JsonUtil.mapper().writeValueAsString(icebergTable.currentSnapshot().summary());
      Assertions.assertEquals(summary, parameters.get(CURRENT_SNAPSHOT_SUMMARY));
      long snapshotId = icebergTable.currentSnapshot().snapshotId();
      Assertions.assertEquals(String.valueOf(snapshotId), parameters.get(CURRENT_SNAPSHOT_ID));
      Assertions.assertEquals(
          String.valueOf(icebergTable.currentSnapshot().timestampMillis()),
          parameters.get(CURRENT_SNAPSHOT_TIMESTAMP));

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
    Assertions.assertTrue(JsonUtil.mapper().writeValueAsString(summary).length() < 4000);
    Map<String, String> parameters = Maps.newHashMap();
    ops.setSnapshotSummary(parameters, snapshot);
    Assertions.assertEquals(1, parameters.size(), "The snapshot summary must be in parameters");

    // create a snapshot summary whose json string size exceeds the limit
    for (int i = 0; i < 1000; i++) {
      summary.put(String.valueOf(i), "value");
    }
    long summarySize = JsonUtil.mapper().writeValueAsString(summary).length();
    // the limit has been updated to 4000 instead of the default value(32672)
    Assertions.assertTrue(summarySize > 4000 && summarySize < 32672);
    parameters.remove(CURRENT_SNAPSHOT_SUMMARY);
    ops.setSnapshotSummary(parameters, snapshot);
    Assertions.assertEquals(
        0,
        parameters.size(),
        "The snapshot summary must not be in parameters due to the size limit");
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
    Assertions.assertNull(parameters.get(CURRENT_SNAPSHOT_SUMMARY));
    Assertions.assertNull(parameters.get(CURRENT_SNAPSHOT_ID));
    Assertions.assertNull(parameters.get(CURRENT_SNAPSHOT_TIMESTAMP));

    ops.setSchema(metadata, parameters);
    Assertions.assertNull(parameters.get(CURRENT_SCHEMA));

    ops.setPartitionSpec(metadata, parameters);
    Assertions.assertNull(parameters.get(DEFAULT_PARTITION_SPEC));

    ops.setSortOrder(metadata, parameters);
    Assertions.assertNull(parameters.get(DEFAULT_SORT_ORDER));
  }

  @Test
  public void testSetDefaultPartitionSpec() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema).create();
      Assertions.assertFalse(
          hmsTableParameters().containsKey(TableProperties.DEFAULT_PARTITION_SPEC),
          "Must not have default partition spec");

      table.updateSpec().addField(bucket("data", 16)).commit();
      Assertions.assertEquals(
          PartitionSpecParser.toJson(table.spec()),
          hmsTableParameters().get(TableProperties.DEFAULT_PARTITION_SPEC));
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

      Assertions.assertEquals(
          SchemaParser.toJson(table.schema()), hmsTableParameters().get(CURRENT_SCHEMA));

      // add many new fields to make the schema json string exceed the limit
      UpdateSchema updateSchema = table.updateSchema();
      for (int i = 0; i < 600; i++) {
        updateSchema.addColumn("new_col_" + i, Types.StringType.get());
      }
      updateSchema.commit();

      Assertions.assertTrue(SchemaParser.toJson(table.schema()).length() > 32672);
      Assertions.assertNull(hmsTableParameters().get(CURRENT_SCHEMA));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  private Map<String, String> hmsTableParameters() throws TException {
    org.apache.hadoop.hive.metastore.api.Table hmsTable = metastoreClient.getTable(DB_NAME, "tbl");
    return hmsTable.getParameters();
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    HiveCatalog catalogWithSlash = new HiveCatalog();
    String wareHousePath = "s3://bucket/db/tbl";

    catalogWithSlash.initialize(
        "hive_catalog", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, wareHousePath + "/"));
    Assertions.assertEquals(
        wareHousePath,
        catalogWithSlash.getConf().get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
        "Should have trailing slash stripped");
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
            hiveConf);

    try {
      Table table =
          hiveCatalog
              .buildTable(tableIdent, schema)
              .withProperty("key2", "table-key2")
              .withProperty("key3", "table-key3")
              .withProperty("key5", "table-key5")
              .create();

      Assertions.assertEquals(
          "catalog-default-key1",
          table.properties().get("key1"),
          "Table defaults set for the catalog must be added to the table properties.");
      Assertions.assertEquals(
          "table-key2",
          table.properties().get("key2"),
          "Table property must override table default properties set at catalog level.");
      Assertions.assertEquals(
          "catalog-override-key3",
          table.properties().get("key3"),
          "Table property override set at catalog level must override table default"
              + " properties set at catalog level and table property specified.");
      Assertions.assertEquals(
          "catalog-override-key4",
          table.properties().get("key4"),
          "Table override not in table props or defaults should be added to table properties");
      Assertions.assertEquals(
          "table-key5",
          table.properties().get("key5"),
          "Table properties without any catalog level default or override should be added to table"
              + " properties.");
    } finally {
      hiveCatalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testDatabaseLocationWithSlashInWarehouseDir() {
    Configuration conf = new Configuration();
    // With a trailing slash
    conf.set("hive.metastore.warehouse.dir", "s3://bucket/");

    HiveCatalog catalog = new HiveCatalog();
    catalog.setConf(conf);

    Database database = catalog.convertToDatabase(Namespace.of("database"), ImmutableMap.of());

    Assertions.assertEquals("s3://bucket/database.db", database.getLocationUri());
  }

  @Test
  public void testRegisterTable() {
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "t1");
    catalog.createTable(identifier, getTestSchema());
    Table registeringTable = catalog.loadTable(identifier);
    catalog.dropTable(identifier, false);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((HiveTableOperations) ops).currentMetadataLocation();
    Table registeredTable = catalog.registerTable(identifier, metadataLocation);
    assertThat(registeredTable).isNotNull();
    TestHelpers.assertSerializedAndLoadedMetadata(registeringTable, registeredTable);
    String expectedMetadataLocation =
        ((HasTableOperations) registeredTable).operations().current().metadataFileLocation();
    assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    assertThat(catalog.loadTable(identifier)).isNotNull();
    assertThat(catalog.dropTable(identifier)).isTrue();
  }

  @Test
  public void testRegisterExistingTable() {
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "t1");
    catalog.createTable(identifier, getTestSchema());
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((HiveTableOperations) ops).currentMetadataLocation();
    assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: hivedb.t1");
    assertThat(catalog.dropTable(identifier, true)).isTrue();
  }
}
