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
package org.apache.iceberg.delta;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBaseSnapshotDeltaLakeTableAction {
  @Rule public TemporaryFolder temp1 = new TemporaryFolder();
  @Rule public TemporaryFolder temp2 = new TemporaryFolder();
  private String sourceTableLocation;
  private final Configuration testHadoopConf = new Configuration();
  private String newTableLocation;
  private final Catalog testCatalog = new TestCatalog();

  @Before
  public void before() {
    try {
      File sourceFolder = temp1.newFolder();
      File destFolder = temp2.newFolder();
      sourceTableLocation = sourceFolder.toURI().toString();
      newTableLocation = destFolder.toURI().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testRequiredTableIdentifier() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeTableAction(sourceTableLocation)
            .icebergCatalog(testCatalog)
            .deltaLakeConfiguration(testHadoopConf)
            .tableLocation(newTableLocation);
    Assert.assertThrows(
        "Should throw IllegalArgumentException if table identifier is not set",
        IllegalArgumentException.class,
        testAction::execute);
  }

  @Test
  public void testRequiredIcebergCatalog() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeTableAction(sourceTableLocation)
            .as(TableIdentifier.of("test", "test"))
            .deltaLakeConfiguration(testHadoopConf)
            .tableLocation(newTableLocation);

    Assert.assertThrows(
        "Should throw IllegalArgumentException if iceberg catalog is not set",
        IllegalArgumentException.class,
        testAction::execute);
  }

  @Test
  public void testRequiredDeltaLakeConfiguration() {
    SnapshotDeltaLakeTable testAction =
        new BaseSnapshotDeltaLakeTableAction(sourceTableLocation)
            .as(TableIdentifier.of("test", "test"))
            .icebergCatalog(testCatalog)
            .tableLocation(newTableLocation);
    Assert.assertThrows(
        "Should throw IllegalArgumentException if delta lake configuration is not set",
        IllegalArgumentException.class,
        testAction::execute);
  }

  private static class TestCatalog extends BaseMetastoreCatalog {
    TestCatalog() {}

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {}
  }
}
