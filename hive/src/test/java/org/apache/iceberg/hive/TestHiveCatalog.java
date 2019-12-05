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

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveCatalog extends HiveMetastoreTest {
  @Test
  public void testCreateNamespace() throws TException {
    Namespace namespace = Namespace.of("dbname");
    namespace.setParameters("owner", "apache");
    namespace.setParameters("group", "iceberg");

    catalog.createNamespace(namespace);

    Database database = metastoreClient.getDatabase(namespace.toString());
    Map<String, String> dbMeta = catalog.getMetafrpmhiveDb(database);
    Assert.assertTrue(dbMeta.get("owner").equals("apache"));
    Assert.assertTrue(dbMeta.get("group").equals("iceberg"));
    Assert.assertEquals("there no same location for db and namespace",
        dbMeta.get("location"), catalog.nameSpaceToHiveDb(namespace).getLocationUri());
  }

  @Test
  public void testListNamespace() throws TException {
    Namespace namespace1 = Namespace.of("dbname1");
    namespace1.setParameters("owner", "apache1");
    namespace1.setParameters("group", "iceberg1");
    catalog.createNamespace(namespace1);

    Namespace namespace2 = Namespace.of("dbname2");
    namespace2.setParameters("owner", "apache2");
    namespace2.setParameters("group", "iceberg2");
    catalog.createNamespace(namespace2);

    List<Namespace> namespaces = catalog.listNamespaces();

    Assert.assertTrue("hive db not hive the namespace 'dbname1'", namespaces.contains(namespace1));
    Assert.assertTrue("hive db not hive the namespace 'dbname2'", namespaces.contains(namespace2));
    for (Namespace namespace : namespaces) {
      if (namespace.toString().equals(namespace1)) {
        Assert.assertEquals("the metadata is not echo",
            namespace.getParameters("owner"), namespace1.getParameters("owner"));
      }
    }
  }

  @Test
  public void testLoadNamespaceMeta() throws TException {
    Namespace namespace = Namespace.of("dbname_load");
    namespace.setParameters("owner", "apache");
    namespace.setParameters("group", "iceberg");
    catalog.createNamespace(namespace);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));
    Assert.assertEquals("there no same location for db and namespace",
        nameMata.get("location"), catalog.nameSpaceToHiveDb(namespace).getLocationUri());
  }

  @Test
  public void testDropNamespace() throws TException {

    Namespace namespace = Namespace.of("dbname_drop");
    namespace.setParameters("owner", "apache");
    namespace.setParameters("group", "iceberg");
    catalog.createNamespace(namespace);

    Map<String, String> nameMata = catalog.loadNamespaceMetadata(namespace);
    Assert.assertTrue(nameMata.get("owner").equals("apache"));
    Assert.assertTrue(nameMata.get("group").equals("iceberg"));

    Assert.assertTrue("drop namespace " + namespace.toString() + "error", catalog.dropNamespace(namespace));

    AssertHelpers.assertThrows("Unknown namespace " + namespace.toString(),
        org.apache.iceberg.exceptions.NotFoundException.class,
        "Unknown namespace " + namespace.toString(), () -> {
          catalog.loadNamespaceMetadata(namespace);
        });
  }

}
