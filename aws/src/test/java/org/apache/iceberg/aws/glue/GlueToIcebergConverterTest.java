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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Table;

public class GlueToIcebergConverterTest {

  @Test
  public void toNamespace() {
    Database database = Database.builder()
        .name("db")
        .build();
    Namespace namespace = Namespace.of("db");
    Assert.assertEquals(namespace, GlueToIcebergConverter.toNamespace(database));
  }

  @Test
  public void toTableId() {
    Table table = Table.builder()
        .databaseName("db")
        .name("name")
        .build();
    TableIdentifier icebergId = TableIdentifier.of("db", "name");
    Assert.assertEquals(icebergId, GlueToIcebergConverter.toTableId(table));
  }

  @Test
  public void validateTable() {
    Map<String, String> properties = new HashMap<>();
    properties.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Table table = Table.builder()
        .parameters(properties)
        .build();
    GlueToIcebergConverter.validateTable(table, "name");
  }

  @Test
  public void validateTable_icebergPropertyNotFound() {
    Map<String, String> properties = new HashMap<>();
    Table table = Table.builder()
        .parameters(properties)
        .build();
    AssertHelpers.assertThrows("Iceberg property not found",
        ValidationException.class,
        "Input Glue table is not an iceberg table",
        () -> GlueToIcebergConverter.validateTable(table, "name")
    );
  }

  @Test
  public void validateTable_icebergPropertyValueWrong() {
    Map<String, String> properties = new HashMap<>();
    properties.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "other");
    Table table = Table.builder()
        .parameters(properties)
        .build();
    AssertHelpers.assertThrows("Iceberg property value wrong",
        ValidationException.class,
        "Input Glue table is not an iceberg table",
        () -> GlueToIcebergConverter.validateTable(table, "name")
    );
  }
}
