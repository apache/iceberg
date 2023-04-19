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

import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.Table;

public class TestGlueToIcebergConverter {

  @Test
  public void testToNamespace() {
    Database database = Database.builder().name("db").build();
    Namespace namespace = Namespace.of("db");
    Assert.assertEquals(namespace, GlueToIcebergConverter.toNamespace(database));
  }

  @Test
  public void testToTableId() {
    Table table = Table.builder().databaseName("db").name("name").build();
    TableIdentifier icebergId = TableIdentifier.of("db", "name");
    Assert.assertEquals(icebergId, GlueToIcebergConverter.toTableId(table));
  }

  @Test
  public void testValidateTableIcebergPropertyNotFound() {
    Table table = Table.builder().parameters(ImmutableMap.of()).build();

    Assertions.assertThatThrownBy(() -> GlueTableOperations.checkIfTableIsIceberg(table, "name"))
        .isInstanceOf(NoSuchIcebergTableException.class)
        .hasMessage("Input Glue table is not an iceberg table: name (type=null)");
  }

  @Test
  public void testValidateTableIcebergPropertyValueWrong() {
    Map<String, String> properties =
        ImmutableMap.of(BaseMetastoreTableOperations.TABLE_TYPE_PROP, "other");
    Table table = Table.builder().parameters(properties).build();

    Assertions.assertThatThrownBy(() -> GlueTableOperations.checkIfTableIsIceberg(table, "name"))
        .isInstanceOf(NoSuchIcebergTableException.class)
        .hasMessage("Input Glue table is not an iceberg table: name (type=other)");
  }
}
