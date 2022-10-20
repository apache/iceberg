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
package org.apache.iceberg.aws.lakeformation;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.lakeformation.model.Permission;

public class TestLakeFormationMetadataOperations extends LakeFormationTestBase {
  @Test
  public void testCreateAndDropDatabaseSuccessful() {
    String testDbName = getRandomDbName();

    grantCreateDbPermission();
    glueCatalogPrivilegedRole.createNamespace(Namespace.of(testDbName));

    grantDatabasePrivileges(testDbName, Permission.DROP);
    glueCatalogPrivilegedRole.dropNamespace(Namespace.of(testDbName));
  }

  @Test
  public void testCreateDatabaseNoPrivileges() {
    String testDbName = getRandomDbName();
    AssertHelpers.assertThrows(
        "attempt to create a database without CREATE_DATABASE permission should fail",
        AccessDeniedException.class,
        "Insufficient Lake Formation permission(s)",
        () -> glueCatalogPrivilegedRole.createNamespace(Namespace.of(testDbName)));
  }

  @Test
  public void testDropDatabaseNoPrivileges() {
    String testDbName = getRandomDbName();
    lfRegisterPathRoleCreateDb(testDbName);
    try {
      AssertHelpers.assertThrows(
          "attempt to drop a database without DROP permission should fail",
          AccessDeniedException.class,
          "Insufficient Lake Formation permission(s)",
          () -> glueCatalogPrivilegedRole.dropNamespace(Namespace.of(testDbName)));
    } finally {
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testShowDatabasesSuccessful() {
    String testDbName = getRandomDbName();
    lfRegisterPathRoleCreateDb(testDbName);
    grantDatabasePrivileges(testDbName, Permission.ALTER);
    try {
      List<Namespace> namespaces = glueCatalogPrivilegedRole.listNamespaces();
      Assert.assertTrue(namespaces.contains(Namespace.of(testDbName)));
    } finally {
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testCreateTableNoCreateTablePermission() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    grantCreateDbPermission();
    lfRegisterPathRoleCreateDb(testDbName);
    String tableLocation = getTableLocation(testTableName);
    grantDataPathPrivileges(tableLocation);
    try {
      AssertHelpers.assertThrows(
          "attempt to create a table without CREATE_TABLE permission should fail",
          AccessDeniedException.class,
          "Insufficient Lake Formation permission(s)",
          () ->
              glueCatalogPrivilegedRole.createTable(
                  TableIdentifier.of(testDbName, testTableName),
                  schema,
                  partitionSpec,
                  tableLocation,
                  null));
    } finally {
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testShowTablesSuccessful() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    grantTablePrivileges(testDbName, testTableName, Permission.ALTER);
    try {
      List<TableIdentifier> tables = glueCatalogPrivilegedRole.listTables(Namespace.of(testDbName));
      Assert.assertTrue(
          tables.contains(TableIdentifier.of(Namespace.of(testDbName), testTableName)));
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testShowTablesNoPrivileges() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    try {
      AssertHelpers.assertThrows(
          "attempt to show tables without any permissions should fail",
          AccessDeniedException.class,
          "Insufficient Lake Formation permission(s)",
          () -> glueCatalogPrivilegedRole.listTables(Namespace.of(testDbName)));
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testCreateTableNoDataPathPermission() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    grantDatabasePrivileges(testDbName, Permission.CREATE_TABLE);
    try {
      AssertHelpers.assertThrows(
          "attempt to create a table without DATA_LOCATION_ACCESS permission should fail",
          ForbiddenException.class,
          "Glue cannot access the requested resources",
          () ->
              glueCatalogPrivilegedRole.createTable(
                  TableIdentifier.of(testDbName, testTableName),
                  schema,
                  partitionSpec,
                  getTableLocation(testTableName),
                  null));
    } finally {
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testCreateTableSuccess() {
    String testDbName = getRandomDbName();
    lfRegisterPathRoleCreateDb(testDbName);
    String testTableName = getRandomTableName();
    String tableLocation = getTableLocation(testTableName);
    grantDataPathPrivileges(tableLocation);
    grantDatabasePrivileges(testDbName, Permission.CREATE_TABLE);
    try {
      glueCatalogPrivilegedRole.createTable(
          TableIdentifier.of(testDbName, testTableName),
          schema,
          partitionSpec,
          tableLocation,
          null);
    } finally {
      grantTablePrivileges(testDbName, testTableName, Permission.DELETE, Permission.DROP);
      glueCatalogPrivilegedRole.dropTable(TableIdentifier.of(testDbName, testTableName), false);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testDropTableSuccessWhenPurgeIsFalse() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    grantTablePrivileges(testDbName, testTableName, Permission.DROP, Permission.SELECT);
    try {
      glueCatalogPrivilegedRole.dropTable(TableIdentifier.of(testDbName, testTableName), false);
    } finally {
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testDropTableNoDropPermission() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    grantTablePrivileges(testDbName, testTableName, Permission.SELECT);
    try {
      AssertHelpers.assertThrows(
          "attempt to drop a table without DROP permission should fail",
          AccessDeniedException.class,
          "Insufficient Lake Formation permission(s)",
          () ->
              glueCatalogPrivilegedRole.dropTable(
                  TableIdentifier.of(testDbName, testTableName), false));
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testAlterTableSetPropertiesSuccessful() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    Map<String, String> properties = Maps.newHashMap();
    grantTablePrivileges(testDbName, testTableName, Permission.ALTER, Permission.INSERT);
    grantDataPathPrivileges(getTableLocation(testTableName));
    try {
      Table table =
          glueCatalogPrivilegedRole.loadTable(
              TableIdentifier.of(Namespace.of(testDbName), testTableName));
      properties.putAll(table.properties());
      properties.put(
          TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
      UpdateProperties updateProperties = table.updateProperties();
      properties.forEach(updateProperties::set);
      updateProperties.commit();
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testAlterTableSetPropertiesNoDataPathAccess() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    Map<String, String> properties = Maps.newHashMap();
    grantTablePrivileges(testDbName, testTableName, Permission.ALTER, Permission.INSERT);
    try {
      Table table =
          glueCatalogPrivilegedRole.loadTable(
              TableIdentifier.of(Namespace.of(testDbName), testTableName));
      properties.putAll(table.properties());
      properties.put(
          TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
      UpdateProperties updateProperties = table.updateProperties();
      properties.forEach(updateProperties::set);
      AssertHelpers.assertThrows(
          "attempt to alter a table without ALTER permission should fail",
          ForbiddenException.class,
          "Glue cannot access the requested resources",
          updateProperties::commit);
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testAlterTableSetPropertiesNoPrivileges() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    grantDataPathPrivileges(getTableLocation(testTableName));
    try {
      AssertHelpers.assertThrows(
          "attempt to alter a table without ALTER permission should fail",
          AccessDeniedException.class,
          "Insufficient Lake Formation permission(s)",
          () ->
              glueCatalogPrivilegedRole.loadTable(
                  TableIdentifier.of(Namespace.of(testDbName), testTableName)));
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }

  @Test
  public void testAlterTableSetPropertiesNoAlterPermission() {
    String testDbName = getRandomDbName();
    String testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
    Map<String, String> properties = Maps.newHashMap();
    grantTablePrivileges(testDbName, testTableName, Permission.SELECT, Permission.INSERT);
    try {
      Table table =
          glueCatalogPrivilegedRole.loadTable(
              TableIdentifier.of(Namespace.of(testDbName), testTableName));
      properties.putAll(table.properties());
      properties.put(
          TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
      UpdateProperties updateProperties = table.updateProperties();
      properties.forEach(updateProperties::set);
      AssertHelpers.assertThrows(
          "attempt to alter a table without ALTER privileges should fail",
          ForbiddenException.class,
          "Glue cannot access the requested resources",
          updateProperties::commit);
    } finally {
      lfRegisterPathRoleDeleteTable(testDbName, testTableName);
      lfRegisterPathRoleDeleteDb(testDbName);
    }
  }
}
