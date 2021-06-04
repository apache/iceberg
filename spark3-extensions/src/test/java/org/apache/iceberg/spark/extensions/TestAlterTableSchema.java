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
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestAlterTableSchema extends SparkExtensionsTestBase {
  public TestAlterTableSchema(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSetIdentifierFields() {
    sql("CREATE TABLE %s (id bigint NOT NULL, location struct<lon:bigint,lat:bigint>) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start without identifier", table.schema().identifierFieldIds().isEmpty());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS (id)", tableName);
    table.refresh();
    Assert.assertEquals("Should have new identifier field",
        Sets.newHashSet(table.schema().findField("id").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS (id, location.lon)", tableName);
    table.refresh();
    Assert.assertEquals("Should have new identifier field",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(),
            table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS (location.lon)", tableName);
    table.refresh();
    Assert.assertEquals("Should have new identifier field",
        Sets.newHashSet(table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());
  }

  @Test
  public void testDropIdentifierFields() {
    sql("CREATE TABLE %s (id bigint NOT NULL, location struct<lon:bigint,lat:bigint>) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue("Table should start without identifier", table.schema().identifierFieldIds().isEmpty());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS (id, location.lon)", tableName);
    table.refresh();
    Assert.assertEquals("Should have new identifier fields",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(),
            table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS (id)", tableName);
    table.refresh();
    Assert.assertEquals("Should removed identifier field",
        Sets.newHashSet(table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS (id, location.lon)", tableName);
    table.refresh();
    Assert.assertEquals("Should have new identifier fields",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(),
            table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS (id, location.lon)", tableName);
    table.refresh();
    Assert.assertEquals("Should have no identifier field",
        Sets.newHashSet(),
        table.schema().identifierFieldIds());
  }
}
