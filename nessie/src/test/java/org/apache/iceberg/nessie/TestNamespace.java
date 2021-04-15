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

package org.apache.iceberg.nessie;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNamespace extends BaseTestIceberg {
  private static final String BRANCH = "test-namespace";

  public TestNamespace() {
    super(BRANCH);
  }

  @Test
  public void testListNamespaces() {
    createTable(TableIdentifier.parse("a.b.c.t1"));
    createTable(TableIdentifier.parse("a.b.t2"));
    createTable(TableIdentifier.parse("a.t3"));
    createTable(TableIdentifier.parse("b.c.t4"));
    createTable(TableIdentifier.parse("b.t5"));
    createTable(TableIdentifier.parse("t6"));

    List<TableIdentifier> tables = catalog.listTables(Namespace.of("a", "b", "c"));
    Assertions.assertThat(tables).isNotNull().hasSize(1);
    tables = catalog.listTables(Namespace.of("a", "b"));
    Assertions.assertThat(tables).isNotNull().hasSize(2);
    tables = catalog.listTables(Namespace.of("a"));
    Assertions.assertThat(tables).isNotNull().hasSize(3);
    tables = catalog.listTables(null);
    Assertions.assertThat(tables).isNotNull().hasSize(6);

    List<Namespace> namespaces = catalog.listNamespaces();
    Assertions.assertThat(namespaces).isNotNull().hasSize(5);
    namespaces = catalog.listNamespaces(Namespace.of("a"));
    Assertions.assertThat(namespaces).isNotNull().hasSize(3);
    namespaces = catalog.listNamespaces(Namespace.of("a", "b"));
    Assertions.assertThat(namespaces).isNotNull().hasSize(2);
    namespaces = catalog.listNamespaces(Namespace.of("b"));
    Assertions.assertThat(namespaces).isNotNull().hasSize(2);
  }
}
