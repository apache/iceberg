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
package org.apache.iceberg.dell.ecs;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.dell.mock.ecs.EcsS3MockRule;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestEcsTableOperations {

  static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @RegisterExtension public EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void testConcurrentCommit() {
    EcsCatalog catalog1 = createCatalog("test1");
    EcsCatalog catalog2 = createCatalog("test2");

    Table catalog1Table = catalog1.createTable(TableIdentifier.of("t1"), SCHEMA);
    Table catalog2Table = catalog2.loadTable(TableIdentifier.of("t1"));

    // Generate a new version
    catalog1Table.updateProperties().set("a", "a").commit();

    // Use the TableOperations to test the CommitFailedException
    // High level actions, such as Table#updateProperties(), may refresh metadata.
    TableOperations operations = ((HasTableOperations) catalog2Table).operations();
    assertThatThrownBy(
            () ->
                operations.commit(
                    operations.current(),
                    TableMetadata.buildFrom(operations.current())
                        .removeProperties(ImmutableSet.of("a"))
                        .build()))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith("Replace failed, E-Tag")
        .hasMessageContaining("mismatch for table test2.t1");
  }

  public EcsCatalog createCatalog(String name) {
    EcsCatalog ecsCatalog = new EcsCatalog();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, new EcsURI(rule.bucket(), "").location());
    properties.putAll(rule.clientProperties());
    ecsCatalog.initialize(name, properties);
    return ecsCatalog;
  }
}
