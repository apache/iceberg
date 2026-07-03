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
package org.apache.iceberg.view;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestBaseMetastoreViewCatalogLoadView {

  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @Test
  public void loadViewReusesOpsFromExistenceCheck() {
    CountingViewOpsCatalog catalog = new CountingViewOpsCatalog();
    catalog.initialize("test-catalog", ImmutableMap.of());

    TableIdentifier identifier = TableIdentifier.of("ns", "view");
    catalog.createNamespace(identifier.namespace());
    catalog
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(identifier.namespace())
        .withQuery("spark", "select * from ns.tbl")
        .create();

    catalog.newViewOpsCalls = 0;
    catalog.loadView(identifier);

    assertThat(catalog.newViewOpsCalls)
        .as("loadView should reuse the ViewOperations created for the existence check")
        .isEqualTo(1);
  }

  private static class CountingViewOpsCatalog extends InMemoryCatalog {
    private int newViewOpsCalls = 0;

    @Override
    protected ViewOperations newViewOps(TableIdentifier identifier) {
      newViewOpsCalls++;
      return super.newViewOps(identifier);
    }
  }
}
