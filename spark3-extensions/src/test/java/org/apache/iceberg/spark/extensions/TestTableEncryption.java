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

package org.apache.iceberg.spark.extensions;

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestTableEncryption extends SparkExtensionsTestBase {
  public TestTableEncryption(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testEncKey() {

    sql("CREATE TABLE %s (id bigint NOT NULL, " +
        "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start without identifier", table.schema().identifierFieldIds().isEmpty());

    AssertHelpers.assertThrows("Should throw an Unsupported Operation Exception for now",
        UnsupportedOperationException.class,
        tableName + " isKEK: true '12345'",
        () -> sql("ALTER TABLE %s ENCRYPTED WITH KEK BY TABLE KEY '12345'", tableName));
  }
}
