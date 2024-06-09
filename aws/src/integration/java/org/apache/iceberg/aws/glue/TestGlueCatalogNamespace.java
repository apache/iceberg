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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.TableInput;

public class TestGlueCatalogNamespace extends GlueTestBase {

  @Test
  public void testCreateNamespace() {
    String namespace = getRandomName();
    namespaces.add(namespace);
    assertThatThrownBy(() -> glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()))
        .as("namespace does not exist before create")
        .isInstanceOf(EntityNotFoundException.class)
        .hasMessageContaining("not found");
    Map<String, String> properties =
        ImmutableMap.of(
            IcebergToGlueConverter.GLUE_DESCRIPTION_KEY,
            "description",
            IcebergToGlueConverter.GLUE_DB_LOCATION_KEY,
            "s3://location",
            "key",
            "val");
    Namespace ns = Namespace.of(namespace);
    glueCatalog.createNamespace(ns, properties);
    Database database =
        glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    assertThat(database.name()).isEqualTo(namespace);
    assertThat(database.description()).isEqualTo("description");
    assertThat(database.locationUri()).isEqualTo("s3://location");
    assertThat(database.parameters()).containsEntry("key", "val");
    assertThat(glueCatalog.loadNamespaceMetadata(ns)).isEqualTo(properties);
  }

  @Test
  public void testCreateDuplicate() {
    String namespace = createNamespace();
    assertThatThrownBy(() -> glueCatalog.createNamespace(Namespace.of(namespace)))
        .as("should not create namespace with the same name")
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("it already exists in Glue");
  }

  @Test
  public void testCreateBadName() {
    List<Namespace> invalidNamespaces =
        Lists.newArrayList(Namespace.of("db-1"), Namespace.of("db", "db2"));

    for (Namespace namespace : invalidNamespaces) {
      assertThatThrownBy(() -> glueCatalog.createNamespace(namespace))
          .as("should not create namespace with invalid or nested names")
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Cannot convert namespace");
    }
  }

  @Test
  public void testNamespaceExists() {
    String namespace = createNamespace();
    assertThat(glueCatalog.namespaceExists(Namespace.of(namespace))).isTrue();
  }

  @Test
  public void testListNamespace() {
    String namespace = createNamespace();
    List<Namespace> namespaceList = glueCatalog.listNamespaces();
    assertThat(namespaceList).isNotEmpty();
    assertThat(namespaceList).contains(Namespace.of(namespace));
    namespaceList = glueCatalog.listNamespaces(Namespace.of(namespace));
    assertThat(namespaceList).isEmpty();
  }

  @Test
  public void testNamespaceProperties() {
    String namespace = createNamespace();
    // set properties
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    properties.put("key2", "val2");
    properties.put(IcebergToGlueConverter.GLUE_DB_LOCATION_KEY, "s3://test");
    properties.put(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, "description");
    glueCatalog.setProperties(Namespace.of(namespace), properties);
    Database database =
        glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    assertThat(database.parameters()).containsEntry("key", "val").containsEntry("key2", "val2");
    assertThat(database.locationUri()).isEqualTo("s3://test");
    assertThat(database.description()).isEqualTo("description");
    // remove properties
    glueCatalog.removeProperties(
        Namespace.of(namespace),
        Sets.newHashSet(
            "key",
            IcebergToGlueConverter.GLUE_DB_LOCATION_KEY,
            IcebergToGlueConverter.GLUE_DESCRIPTION_KEY));
    database = glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    assertThat(database.parameters()).doesNotContainKey("key").containsEntry("key2", "val2");
    assertThat(database.locationUri()).isNull();
    assertThat(database.description()).isNull();
    // add back
    properties = Maps.newHashMap();
    properties.put("key", "val");
    properties.put(IcebergToGlueConverter.GLUE_DB_LOCATION_KEY, "s3://test2");
    properties.put(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, "description2");
    glueCatalog.setProperties(Namespace.of(namespace), properties);
    database = glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()).database();
    assertThat(database.parameters()).containsEntry("key", "val").containsEntry("key2", "val2");
    assertThat(database.locationUri()).isEqualTo("s3://test2");
    assertThat(database.description()).isEqualTo("description2");
  }

  @Test
  public void testDropNamespace() {
    String namespace = createNamespace();
    glueCatalog.dropNamespace(Namespace.of(namespace));
    assertThatThrownBy(() -> glue.getDatabase(GetDatabaseRequest.builder().name(namespace).build()))
        .as("namespace should not exist after deletion")
        .isInstanceOf(EntityNotFoundException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testDropNamespaceThatContainsOnlyIcebergTable() {
    String namespace = createNamespace();
    createTable(namespace);
    assertThatThrownBy(() -> glueCatalog.dropNamespace(Namespace.of(namespace)))
        .as("namespace should not be dropped when still has Iceberg table")
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessageContaining("still contains Iceberg tables");
  }

  @Test
  public void testDropNamespaceThatContainsNonIcebergTable() {
    String namespace = createNamespace();
    glue.createTable(
        CreateTableRequest.builder()
            .databaseName(namespace)
            .tableInput(TableInput.builder().name(UUID.randomUUID().toString()).build())
            .build());
    assertThatThrownBy(() -> glueCatalog.dropNamespace(Namespace.of(namespace)))
        .as("namespace should not be dropped when still has non-Iceberg table")
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessageContaining("still contains non-Iceberg tables");
  }
}
