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
package org.apache.iceberg.spark.sql;

import java.util.Map;
import org.apache.iceberg.Table;
import org.junit.Before;

public class TestPartitionedWritesToBranch extends PartitionedWritesTestBase {

  private static final String BRANCH = "test";

  public TestPartitionedWritesToBranch(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  @Override
  public void createTables() {
    super.createTables();
    Table table = validationCatalog.loadTable(tableIdent);
    table.manageSnapshots().createBranch(BRANCH, table.currentSnapshot().snapshotId()).commit();
    sql("REFRESH TABLE " + tableName);
  }

  @Override
  protected String commitTarget() {
    return String.format("%s.branch_%s", tableName, BRANCH);
  }

  @Override
  protected String selectTarget() {
    return String.format("%s VERSION AS OF '%s'", tableName, BRANCH);
  }
}
