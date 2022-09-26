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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.lakeformation.model.Permission;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestLakeFormationDataOperations extends LakeFormationTestBase {

  private static String testDbName;
  private static String testTableName;

  @Before
  public void before() {
    testDbName = getRandomDbName();
    testTableName = getRandomTableName();
    lfRegisterPathRoleCreateDb(testDbName);
    lfRegisterPathRoleCreateTable(testDbName, testTableName);
  }

  @After
  public void after() {
    lfRegisterPathRoleDeleteTable(testDbName, testTableName);
    lfRegisterPathRoleDeleteDb(testDbName);
  }

  @Test
  public void testLoadTableWithNoTableAccess() {
    AssertHelpers.assertThrows(
        "attempt to load a table without SELECT permission should fail",
        AccessDeniedException.class,
        "Insufficient Lake Formation permission(s)",
        () ->
            glueCatalogPrivilegedRole.loadTable(
                TableIdentifier.of(Namespace.of(testDbName), testTableName)));
  }

  @Test
  public void testLoadTableSuccess() {
    grantTablePrivileges(testDbName, testTableName, Permission.SELECT);
    glueCatalogPrivilegedRole.loadTable(
        TableIdentifier.of(Namespace.of(testDbName), testTableName));
  }

  @Test
  public void testUpdateTableWithNoInsertAccess() {
    grantTablePrivileges(testDbName, testTableName, Permission.SELECT);
    Table table =
        glueCatalogPrivilegedRole.loadTable(
            TableIdentifier.of(Namespace.of(testDbName), testTableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath(getTableLocation(testTableName) + "/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    AssertHelpers.assertThrows(
        "attempt to insert to a table without INSERT permission should fail",
        S3Exception.class,
        "Access Denied",
        () -> table.newAppend().appendFile(dataFile).commit());
  }

  @Test
  public void testUpdateTableSuccess() {
    grantTablePrivileges(
        testDbName, testTableName, Permission.SELECT, Permission.ALTER, Permission.INSERT);
    grantDataPathPrivileges(getTableLocation(testTableName));
    Table table =
        glueCatalogPrivilegedRole.loadTable(
            TableIdentifier.of(Namespace.of(testDbName), testTableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath(getTableLocation(testTableName) + "/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
  }

  @Test
  public void testDeleteWithNoDataPathAccess() {
    grantTablePrivileges(
        testDbName, testTableName, Permission.SELECT, Permission.INSERT, Permission.ALTER);
    Table table =
        glueCatalogPrivilegedRole.loadTable(
            TableIdentifier.of(Namespace.of(testDbName), testTableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath(getTableLocation(testTableName) + "/path/to/delete-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    AssertHelpers.assertThrows(
        "attempt to delete without DATA_LOCATION_ACCESS permission should fail",
        ForbiddenException.class,
        "Glue cannot access the requested resources",
        () -> table.newDelete().deleteFile(dataFile).commit());
  }

  @Test
  public void testDeleteSuccess() {
    grantTablePrivileges(
        testDbName, testTableName, Permission.SELECT, Permission.ALTER, Permission.INSERT);
    grantDataPathPrivileges(getTableLocation(testTableName));
    Table table =
        glueCatalogPrivilegedRole.loadTable(
            TableIdentifier.of(Namespace.of(testDbName), testTableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath(getTableLocation(testTableName) + "/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newDelete().deleteFile(dataFile).commit();
  }
}
