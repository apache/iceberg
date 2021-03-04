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

package org.apache.iceberg.aws.glue;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.TableInput;

public class GlueCatalogNamespaceTest extends GlueTestBase {

  @Test
  public void testCreateNamespace() {
    String namespace = getRandomName();
    namespaces.add(namespace);
    AssertHelpers.assertThrows("namespace does not exist before create",
        EntityNotFoundException.class,
        "not found",
        () -> glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()));
    glueCatalog.createNamespace(Namespace.of(namespace));
    Database database = glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    Assert.assertEquals("namespace must equal database name", namespace, database.name());
  }

  @Test
  public void testCreateDuplicate() {
    String namespace = createNamespace();
    AssertHelpers.assertThrows("should not create namespace with the same name",
        AlreadyExistsException.class,
        "it already exists in Glue",
        () -> glueCatalog.createNamespace(Namespace.of(namespace)));
  }

  @Test
  public void testCreateBadName() {
    List<Namespace> invalidNamespaces = Lists.newArrayList(
        Namespace.of("db-1"),
        Namespace.of("db", "db2")
    );

    for (Namespace namespace : invalidNamespaces) {
      AssertHelpers.assertThrows("should not create namespace with invalid or nested names",
          ValidationException.class,
          "Cannot convert namespace",
          () -> glueCatalog.createNamespace(namespace));
    }
  }

  @Test
  public void testNamespaceExists() {
    String namespace = createNamespace();
    Assert.assertTrue(glueCatalog.namespaceExists(Namespace.of(namespace)));
  }

  @Test
  public void testListNamespace() {
    String namespace = createNamespace();
    List<Namespace> namespaceList = glueCatalog.listNamespaces();
    Assert.assertTrue(namespaceList.size() > 0);
    Assert.assertTrue(namespaceList.contains(Namespace.of(namespace)));
    namespaceList = glueCatalog.listNamespaces(Namespace.of(namespace));
    Assert.assertTrue(namespaceList.isEmpty());
  }

  @Test
  public void testNamespaceProperties() {
    String namespace = createNamespace();
    // set properties
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    properties.put("key2", "val2");
    glueCatalog.setProperties(Namespace.of(namespace), properties);
    Database database = glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    Assert.assertTrue(database.parameters().containsKey("key"));
    Assert.assertEquals("val", database.parameters().get("key"));
    Assert.assertTrue(database.parameters().containsKey("key2"));
    Assert.assertEquals("val2", database.parameters().get("key2"));
    // remove properties
    glueCatalog.removeProperties(Namespace.of(namespace), Sets.newHashSet("key"));
    database = glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    Assert.assertFalse(database.parameters().containsKey("key"));
    Assert.assertTrue(database.parameters().containsKey("key2"));
    Assert.assertEquals("val2", database.parameters().get("key2"));
    // add back
    properties = Maps.newHashMap();
    properties.put("key", "val");
    glueCatalog.setProperties(Namespace.of(namespace), properties);
    database = glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    Assert.assertTrue(database.parameters().containsKey("key"));
    Assert.assertEquals("val", database.parameters().get("key"));
    Assert.assertTrue(database.parameters().containsKey("key2"));
    Assert.assertEquals("val2", database.parameters().get("key2"));
  }

  @Test
  public void testDropNamespace() {
    String namespace = createNamespace();
    glueCatalog.dropNamespace(Namespace.of(namespace));
    AssertHelpers.assertThrows("namespace should not exist after deletion",
        EntityNotFoundException.class,
        "not found",
        () -> glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()));
  }

  @Test
  public void testDropNamespaceNonEmpty_containsIcebergTable() {
    String namespace = createNamespace();
    createTable(namespace);
    AssertHelpers.assertThrows("namespace should not be dropped when still has Iceberg table",
        NamespaceNotEmptyException.class,
        "still contains Iceberg tables",
        () -> glueCatalog.dropNamespace(Namespace.of(namespace)));
  }

  @Test
  public void testDropNamespaceNonEmpty_containsNonIcebergTable() {
    String namespace = createNamespace();
    glue.createTable(CreateTableRequest.builder()
        .databaseName(namespace)
        .tableInput(TableInput.builder()
            .name(UUID.randomUUID().toString())
            .build())
        .build());
    AssertHelpers.assertThrows("namespace should not be dropped when still has non-Iceberg table",
        NamespaceNotEmptyException.class,
        "still contains non-Iceberg tables",
        () -> glueCatalog.dropNamespace(Namespace.of(namespace)));
  }

}
