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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.iceberg.AssertHelpers;
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

public class TestHiveCatalog extends HiveMetastoreTest {
  private static ImmutableMap meta =
      ImmutableMap.of(
          "owner", "apache",
          "group", "iceberg",
          "comment", "iceberg  hiveCatalog test");

  @Rule public TemporaryFolder temp = new TemporaryFolder();

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
    String location = temp.newFolder("tbl").toString();

    try {
      Table table =
          catalog
              .buildTable(tableIdent, schema)
              .withPartitionSpec(spec)
              .withLocation(location)
              .withProperty("key1", "value1")
              .withProperty("key2", "value2")
              .create();

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertEquals(1, table.spec().fields().size());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableWithCaching() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();
    ImmutableMap<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    Catalog cachingCatalog = CachingCatalog.wrap(catalog);

    try {
      Table table = cachingCatalog.createTable(tableIdent, schema, spec, location, properties);

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertEquals(1, table.spec().fields().size());
      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
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

    Assert.assertEquals(catalog.getConf().get("hive.metastore.uris"), "thrift://examplehost:9083");
    Assert.assertEquals(
        catalog.getConf().get("hive.metastore.warehouse.dir"), "/user/hive/testwarehouse");
  }

  @Test
  public void testCreateTableTxnBuilder() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();

    try {
      Transaction txn =
          catalog.buildTable(tableIdent, schema).withLocation(location).createTransaction();
      txn.commitTransaction();
      Table table = catalog.loadTable(tableIdent);

      Assert.assertEquals(location, table.location());
      Assert.assertEquals(2, table.schema().columns().size());
      Assert.assertTrue(table.spec().isUnpartitioned());
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testReplaceTxnBuilder() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();

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
      Assert.assertEquals(1, table.spec().fields().size());

      String newLocation = temp.newFolder("tbl-2").toString();

      Transaction replaceTxn =
          catalog
              .buildTable(tableIdent, schema)
              .withProperty("key2", "value2")
              .withLocation(newLocation)
              .replaceTransaction();
      replaceTxn.commitTransaction();

      table = catalog.loadTable(tableIdent);
      Assert.assertEquals(newLocation, table.location());
      Assert.assertNull(table.currentSnapshot());
      PartitionSpec v1Expected =
          PartitionSpec.builderFor(table.schema())
              .alwaysNull("data", "data_bucket")
              .withSpecId(1)
              .build();
      Assert.assertEquals("Table should have a spec with one void field", v1Expected, table.spec());

      Assert.assertEquals("value1", table.properties().get("key1"));
      Assert.assertEquals("value2", table.properties().get("key2"));
    } finally {
      catalog.dropTable(tableIdent);
    }
  }

  @Test
  public void testCreateTableWithOwner() throws Exception {
    Schema schema = getTestSchema();
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("data", 16).build();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();
    String owner = "some_owner";
    ImmutableMap<String, String> properties = ImmutableMap.of(HiveCatalog.HMS_TABLE_OWNER, owner);

    try {
      Table table = catalog.createTable(tableIdent, schema, spec, location, properties);
      org.apache.hadoop.hive.metastore.api.Table hmsTable =
          metastoreClient.getTable(DB_NAME, "tbl");
      Assert.assertEquals(owner, hmsTable.getOwner());
      Map<String, String> hmsTableParams = hmsTable.getParameters();
      Assert.assertFalse(hmsTableParams.containsKey(HiveCatalog.HMS_TABLE_OWNER));
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
      Assert.assertEquals("Order ID must match", 0, table.sortOrder().orderId());
      Assert.assertTrue("Order must unsorted", table.sortOrder().isUnsorted());

      Assert.assertFalse(
          "Must not have default sort order in catalog",
          hmsTableParameters().containsKey(DEFAULT_SORT_ORDER));
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
      Assert.assertEquals("Order ID must match", 1, sortOrder.orderId());
      Assert.assertEquals("Order must have 1 field", 1, sortOrder.fields().size());
      Assert.assertEquals("Direction must match ", ASC, sortOrder.fields().get(0).direction());
      Assert.assertEquals(
          "Null order must match ", NULLS_FIRST, sortOrder.fields().get(0).nullOrder());
      Transform<?, ?> transform = Transforms.identity(Types.IntegerType.get());
      Assert.assertEquals("Transform must match", transform, sortOrder.fields().get(0).transform());

      Assert.assertEquals(
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

    Assert.assertTrue(database1.getParameters().get("owner").equals("apache"));
    Assert.assertTrue(database1.getParameters().get("group").equals("iceberg"));

    Assert.assertEquals(
        "There no same location for db and namespace",
        database1.getLocationUri(),
        defaultUri(namespace1));

    AssertHelpers.assertThrows(
        "Should fail to create when namespace already exist " + namespace1,
        AlreadyExistsException.class,
        "Namespace '" + namespace1 + "' already exists!",
        () -> {
          catalog.createNamespace(namespace1);
        });
    String hiveLocalDir = temp.newFolder().toURI().toString();
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
    Assert.assertEquals(
        "There no same location for db and namespace", database2.getLocationUri(), hiveLocalDir);
  }

  @Test
  public void testCreateNamespaceWithOwnership() throws Exception {
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

    AssertHelpers.assertThrows(
        String.format(
            "Create namespace setting %s without setting %s is not allowed",
            HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER),
        IllegalArgumentException.class,
        () -> {
          try {
            createNamespaceAndVerifyOwnership(
                "create_with_owner_type_alone",
                ImmutableMap.of(HiveCatalog.HMS_DB_OWNER_TYPE, PrincipalType.USER.name()),
                "no_post_create_expectation_due_to_exception_thrown",
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });

    AssertHelpers.assertThrows(
        "No enum constant " + PrincipalType.class.getCanonicalName(),
        IllegalArgumentException.class,
        () -> {
          try {
            createNamespaceAndVerifyOwnership(
                "create_with_invalid_owner_type",
                ImmutableMap.of(
                    HiveCatalog.HMS_DB_OWNER, "iceberg",
                    HiveCatalog.HMS_DB_OWNER_TYPE, "invalidOwnerType"),
                "no_post_create_expectation_due_to_exception_thrown",
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });
  }

  private void createNamespaceAndVerifyOwnership(
      String name, Map<String, String> prop, String expectedOwner, PrincipalType expectedOwnerType)
      throws TException {
    Namespace namespace = Namespace.of(name);

    catalog.createNamespace(namespace, prop);
    Database db = metastoreClient.getDatabase(namespace.toString());

    Assert.assertEquals(expectedOwner, db.getOwnerName());
    Assert.assertEquals(expectedOwnerType, db.getOwnerType());
  }

  @Test
  public void testListNamespace() throws TException {
    List<Namespace> namespaces;
    Namespace namespace1 = Namespace.of("dbname1");
    catalog.createNamespace(namespace1, meta);
    namespaces = catalog.listNamespaces(namespace1);
    Assert.assertTrue("Hive db not hive the namespace 'dbname1'", namespaces.isEmpty());

    Namespace namespace2 = Namespace.of("dbname2");
    catalog.createNamespace(namespace2, meta);
    namespaces = catalog.listNamespaces();

    Assert.assertTrue("Hive db not hive the namespace 'dbname2'", namespaces.contains(namespace2));
  }

  @Test
  public void testLoadNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_load");

    catalog.createNamespace(namespace, meta);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));
    Assert.assertEquals(
        "There no same location for db and namespace",
        nameMata.get("location"),
        catalog.convertToDatabase(namespace, meta).getLocationUri());
  }

  @Test
  public void testNamespaceExists() throws TException {
    Namespace namespace = Namespace.of("dbname_exists");

    catalog.createNamespace(namespace, meta);

    Assert.assertTrue("Should true to namespace exist", catalog.namespaceExists(namespace));
    Assert.assertTrue(
        "Should false to namespace doesn't exist",
        !catalog.namespaceExists(Namespace.of("db2", "db2", "ns2")));
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
    Assert.assertEquals(database.getParameters().get("owner"), "alter_apache");
    Assert.assertEquals(database.getParameters().get("test"), "test");
    Assert.assertEquals(database.getParameters().get("group"), "iceberg");
    AssertHelpers.assertThrows(
        "Should fail to namespace not exist" + namespace,
        NoSuchNamespaceException.class,
        "Namespace does not exist: ",
        () -> {
          catalog.setProperties(Namespace.of("db2", "db2", "ns2"), meta);
        });
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

    AssertHelpers.assertThrows(
        String.format(
            "Setting %s and %s has to be performed together or not at all",
            HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER),
        IllegalArgumentException.class,
        () -> {
          try {
            setNamespaceOwnershipAndVerify(
                "set_owner_without_setting_owner_type",
                ImmutableMap.of(),
                ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_individual_owner"),
                System.getProperty("user.name"),
                PrincipalType.USER,
                "no_post_setting_expectation_due_to_exception_thrown",
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });

    AssertHelpers.assertThrows(
        String.format(
            "Setting %s and %s has to be performed together or not at all",
            HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER),
        IllegalArgumentException.class,
        () -> {
          try {
            setNamespaceOwnershipAndVerify(
                "set_owner_type_without_setting_owner",
                ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_owner"),
                ImmutableMap.of(HiveCatalog.HMS_DB_OWNER_TYPE, PrincipalType.GROUP.name()),
                "some_owner",
                PrincipalType.USER,
                "no_post_setting_expectation_due_to_exception_thrown",
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });

    AssertHelpers.assertThrows(
        HiveCatalog.HMS_DB_OWNER_TYPE
            + " has an invalid value of: "
            + meta.get(HiveCatalog.HMS_DB_OWNER_TYPE)
            + ". Acceptable values are: "
            + Stream.of(PrincipalType.values()).map(Enum::name).collect(Collectors.joining(", ")),
        IllegalArgumentException.class,
        () -> {
          try {
            setNamespaceOwnershipAndVerify(
                "set_invalid_owner_type",
                ImmutableMap.of(),
                ImmutableMap.of(
                    HiveCatalog.HMS_DB_OWNER, "iceberg",
                    HiveCatalog.HMS_DB_OWNER_TYPE, "invalidOwnerType"),
                System.getProperty("user.name"),
                PrincipalType.USER,
                "no_post_setting_expectation_due_to_exception_thrown",
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });
  }

  @Test
  public void testSetNamespaceOwnershipNoop() throws TException {
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
        System.getProperty("user.name"),
        PrincipalType.USER,
        System.getProperty("user.name"),
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

    Assert.assertEquals(expectedOwnerPostSet, database.getOwnerName());
    Assert.assertEquals(expectedOwnerTypePostSet, database.getOwnerType());
  }

  @Test
  public void testRemoveNamespaceProperties() throws TException {
    Namespace namespace = Namespace.of("dbname_remove");

    catalog.createNamespace(namespace, meta);

    catalog.removeProperties(namespace, ImmutableSet.of("comment", "owner"));

    Database database = metastoreClient.getDatabase(namespace.level(0));

    Assert.assertEquals(database.getParameters().get("owner"), null);
    Assert.assertEquals(database.getParameters().get("group"), "iceberg");
    AssertHelpers.assertThrows(
        "Should fail to namespace not exist" + namespace,
        NoSuchNamespaceException.class,
        "Namespace does not exist: ",
        () -> {
          catalog.removeProperties(
              Namespace.of("db2", "db2", "ns2"), ImmutableSet.of("comment", "owner"));
        });
  }

  @Test
  public void testRemoveNamespaceOwnership() throws TException {
    removeNamespaceOwnershipAndVerify(
        "remove_individual_ownership",
        ImmutableMap.of(HiveCatalog.HMS_DB_OWNER, "some_owner"),
        ImmutableSet.of(HiveCatalog.HMS_DB_OWNER, HiveCatalog.HMS_DB_OWNER_TYPE),
        "some_owner",
        PrincipalType.USER,
        System.getProperty("user.name"),
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
        System.getProperty("user.name"),
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_ownership_on_default_noop_1",
        ImmutableMap.of(),
        ImmutableSet.of(HiveCatalog.HMS_DB_OWNER, HiveCatalog.HMS_DB_OWNER_TYPE),
        System.getProperty("user.name"),
        PrincipalType.USER,
        System.getProperty("user.name"),
        PrincipalType.USER);

    removeNamespaceOwnershipAndVerify(
        "remove_ownership_on_default_noop_2",
        ImmutableMap.of(),
        ImmutableSet.of(),
        System.getProperty("user.name"),
        PrincipalType.USER,
        System.getProperty("user.name"),
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

    AssertHelpers.assertThrows(
        String.format(
            "Removing %s and %s has to be performed together or not at all",
            HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER),
        IllegalArgumentException.class,
        () -> {
          try {
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
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });

    AssertHelpers.assertThrows(
        String.format(
            "Removing %s and %s has to be performed together or not at all",
            HiveCatalog.HMS_DB_OWNER_TYPE, HiveCatalog.HMS_DB_OWNER),
        IllegalArgumentException.class,
        () -> {
          try {
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
                null);
          } catch (TException e) {
            throw new RuntimeException("Unexpected Exception", e);
          }
        });
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

    Assert.assertEquals(expectedOwnerPostRemove, database.getOwnerName());
    Assert.assertEquals(expectedOwnerTypePostRemove, database.getOwnerType());
  }

  @Test
  public void testDropNamespace() throws TException {
    Namespace namespace = Namespace.of("dbname_drop");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");
    Schema schema = getTestSchema();

    catalog.createNamespace(namespace, meta);
    catalog.createTable(identifier, schema);
    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));

    AssertHelpers.assertThrows(
        "Should fail to drop namespace is not empty" + namespace,
        NamespaceNotEmptyException.class,
        "Namespace dbname_drop is not empty. One or more tables exist.",
        () -> {
          catalog.dropNamespace(namespace);
        });
    Assert.assertTrue(catalog.dropTable(identifier, true));
    Assert.assertTrue(
        "Should fail to drop namespace if it is not empty", catalog.dropNamespace(namespace));
    Assert.assertFalse(
        "Should fail to drop when namespace doesn't exist",
        catalog.dropNamespace(Namespace.of("db.ns1")));
    AssertHelpers.assertThrows(
        "Should fail to drop namespace exist" + namespace,
        NoSuchNamespaceException.class,
        "Namespace does not exist: ",
        () -> {
          catalog.loadNamespaceMetadata(namespace);
        });
  }

  @Test
  public void testDropTableWithoutMetadataFile() {
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "tbl");
    Schema tableSchema = getTestSchema();
    catalog.createTable(identifier, tableSchema);
    String metadataFileLocation = catalog.newTableOps(identifier).current().metadataFileLocation();
    TableOperations ops = catalog.newTableOps(identifier);
    ops.io().deleteFile(metadataFileLocation);
    Assert.assertTrue(catalog.dropTable(identifier));
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
      Assert.assertEquals("Name must match", "hive.hivedb.tbl", table.name());

      TableIdentifier snapshotsTableIdent = TableIdentifier.of(DB_NAME, "tbl", "snapshots");
      Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
      Assert.assertEquals("Name must match", "hive.hivedb.tbl.snapshots", snapshotsTable.name());
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
    String location = temp.newFolder("tbl").toString();

    try {
      catalog.buildTable(tableIdentifier, schema).withLocation(location).create();

      Assert.assertNotNull(hmsTableParameters().get(TableProperties.UUID));
    } finally {
      catalog.dropTable(tableIdentifier);
    }
  }

  @Test
  public void testSnapshotStatsTableProperties() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdentifier = TableIdentifier.of(DB_NAME, "tbl");
    String location = temp.newFolder("tbl").toString();

    try {
      catalog.buildTable(tableIdentifier, schema).withLocation(location).create();

      // check whether parameters are in expected state
      Map<String, String> parameters = hmsTableParameters();
      Assert.assertEquals("0", parameters.get(TableProperties.SNAPSHOT_COUNT));
      Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_SUMMARY));
      Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_ID));
      Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_TIMESTAMP));

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
      Assert.assertEquals("1", parameters.get(TableProperties.SNAPSHOT_COUNT));
      String summary =
          JsonUtil.mapper().writeValueAsString(icebergTable.currentSnapshot().summary());
      Assert.assertEquals(summary, parameters.get(CURRENT_SNAPSHOT_SUMMARY));
      long snapshotId = icebergTable.currentSnapshot().snapshotId();
      Assert.assertEquals(String.valueOf(snapshotId), parameters.get(CURRENT_SNAPSHOT_ID));
      Assert.assertEquals(
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
    Assert.assertTrue(JsonUtil.mapper().writeValueAsString(summary).length() < 4000);
    Map<String, String> parameters = Maps.newHashMap();
    ops.setSnapshotSummary(parameters, snapshot);
    Assert.assertEquals("The snapshot summary must be in parameters", 1, parameters.size());

    // create a snapshot summary whose json string size exceeds the limit
    for (int i = 0; i < 1000; i++) {
      summary.put(String.valueOf(i), "value");
    }
    long summarySize = JsonUtil.mapper().writeValueAsString(summary).length();
    // the limit has been updated to 4000 instead of the default value(32672)
    Assert.assertTrue(summarySize > 4000 && summarySize < 32672);
    parameters.remove(CURRENT_SNAPSHOT_SUMMARY);
    ops.setSnapshotSummary(parameters, snapshot);
    Assert.assertEquals(
        "The snapshot summary must not be in parameters due to the size limit",
        0,
        parameters.size());
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
    Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_SUMMARY));
    Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_ID));
    Assert.assertNull(parameters.get(CURRENT_SNAPSHOT_TIMESTAMP));

    ops.setSchema(metadata, parameters);
    Assert.assertNull(parameters.get(CURRENT_SCHEMA));

    ops.setPartitionSpec(metadata, parameters);
    Assert.assertNull(parameters.get(DEFAULT_PARTITION_SPEC));

    ops.setSortOrder(metadata, parameters);
    Assert.assertNull(parameters.get(DEFAULT_SORT_ORDER));
  }

  @Test
  public void testSetDefaultPartitionSpec() throws Exception {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(DB_NAME, "tbl");

    try {
      Table table = catalog.buildTable(tableIdent, schema).create();
      Assert.assertFalse(
          "Must not have default partition spec",
          hmsTableParameters().containsKey(TableProperties.DEFAULT_PARTITION_SPEC));

      table.updateSpec().addField(bucket("data", 16)).commit();
      Assert.assertEquals(
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

      Assert.assertEquals(
          SchemaParser.toJson(table.schema()), hmsTableParameters().get(CURRENT_SCHEMA));

      // add many new fields to make the schema json string exceed the limit
      UpdateSchema updateSchema = table.updateSchema();
      for (int i = 0; i < 600; i++) {
        updateSchema.addColumn("new_col_" + i, Types.StringType.get());
      }
      updateSchema.commit();

      Assert.assertTrue(SchemaParser.toJson(table.schema()).length() > 32672);
      Assert.assertNull(hmsTableParameters().get(CURRENT_SCHEMA));
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
    Assert.assertEquals(
        "Should have trailing slash stripped",
        wareHousePath,
        catalogWithSlash.getConf().get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));
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

      Assert.assertEquals(
          "Table defaults set for the catalog must be added to the table properties.",
          "catalog-default-key1",
          table.properties().get("key1"));
      Assert.assertEquals(
          "Table property must override table default properties set at catalog level.",
          "table-key2",
          table.properties().get("key2"));
      Assert.assertEquals(
          "Table property override set at catalog level must override table default"
              + " properties set at catalog level and table property specified.",
          "catalog-override-key3",
          table.properties().get("key3"));
      Assert.assertEquals(
          "Table override not in table props or defaults should be added to table properties",
          "catalog-override-key4",
          table.properties().get("key4"));
      Assert.assertEquals(
          "Table properties without any catalog level default or override should be added to table"
              + " properties.",
          "table-key5",
          table.properties().get("key5"));
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

    Assert.assertEquals("s3://bucket/database.db", database.getLocationUri());
  }
}
