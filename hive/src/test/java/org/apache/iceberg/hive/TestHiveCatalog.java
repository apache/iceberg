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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.NamespaceChange;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveCatalog extends HiveMetastoreTest {
  private static final String hiveLocalDir = "file:/tmp/hive/" + UUID.randomUUID().toString();
  private static ImmutableMap meta = ImmutableMap.of(
      "owner", "apache",
      "group", "iceberg",
      "comment", "iceberg  hiveCatalog test");

  @Test
  public void testCreateNamespace() throws TException {
    Namespace namespace1 = Namespace.of("noLocation");
    catalog.createNamespace(namespace1, meta);
    Database database1 = metastoreClient.getDatabase(namespace1.toString());

    Assert.assertTrue(database1.getParameters().get("owner").equals("apache"));
    Assert.assertTrue(database1.getParameters().get("group").equals("iceberg"));

    Assert.assertEquals("There no same location for db and namespace",
        database1.getLocationUri(), defaultUri(namespace1));

    AssertHelpers.assertThrows("Should fail to create when namespace already exist " + namespace1,
        org.apache.iceberg.exceptions.AlreadyExistsException.class,
        "Namespace '" + namespace1 + "' already exists!", () -> {
          catalog.createNamespace(namespace1);
        });
    ImmutableMap newMeta = ImmutableMap.<String, String>builder()
        .putAll(meta)
        .put("location", hiveLocalDir)
        .build();
    Namespace namespace2 = Namespace.of("haveLocation");

    catalog.createNamespace(namespace2, newMeta);
    Database database2 = metastoreClient.getDatabase(namespace2.toString());
    Assert.assertEquals("There no same location for db and namespace",
        database2.getLocationUri(), hiveLocalDir);
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
    Assert.assertEquals("There no same location for db and namespace",
        nameMata.get("location"), catalog.convertToDatabase(namespace, meta).getLocationUri());
  }

  @Test
  public void testAlterNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_alter");

    catalog.createNamespace(namespace, meta);
    catalog.alterNamespace(namespace,
        NamespaceChange.setProperty("owner", "alter_apache"),
        NamespaceChange.setProperty("test", "test"),
        NamespaceChange.setProperty("location", "file:/data/tmp"),
        NamespaceChange.removeProperty("test3"),
        NamespaceChange.removeProperty("group")
    );

    Database database = metastoreClient.getDatabase(namespace.level(0));

    Assert.assertTrue(database.getParameters().get("owner").equals("alter_apache"));
    Assert.assertTrue(database.getParameters().get("test").equals("test"));
    Assert.assertEquals(database.getParameters().get("group"), null);
    Assert.assertEquals(database.getParameters().get("test3"), null);
  }

  @Test
  public void testDropNamespace() throws TException {
    Namespace namespace = Namespace.of("dbname_drop");
    catalog.createNamespace(namespace, meta);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));

    Assert.assertTrue("Drop namespace " + namespace + " error ", catalog.dropNamespace(namespace));
    AssertHelpers.assertThrows("Should fail to drop when namespace doesn't exist", NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
          catalog.dropNamespace(Namespace.of("db.ns1"));
        });
    AssertHelpers.assertThrows("Should fail to drop namespace exist" + namespace,
        org.apache.iceberg.exceptions.NoSuchNamespaceException.class,
        "Namespace does not exist: ", () -> {
          catalog.loadNamespaceMetadata(namespace);
        });
  }

  private String defaultUri(Namespace namespace) throws TException {
    return metastoreClient.getConfigValue(
        "hive.metastore.warehouse.dir", "") +  "/" + namespace.level(0) + ".db";
  }

}
