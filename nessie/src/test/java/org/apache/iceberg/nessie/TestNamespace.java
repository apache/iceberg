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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    assertEquals(1, tables.size());
    tables = catalog.listTables(Namespace.of("a", "b"));
    assertEquals(2, tables.size());
    tables = catalog.listTables(Namespace.of("a"));
    assertEquals(3, tables.size());
    tables = catalog.listTables(null);
    assertEquals(6, tables.size());

    List<Namespace> namespaces = catalog.listNamespaces();
    assertEquals(5, namespaces.size());
    namespaces = catalog.listNamespaces(Namespace.of("a"));
    assertEquals(3, namespaces.size());
    namespaces = catalog.listNamespaces(Namespace.of("a", "b"));
    assertEquals(2, namespaces.size());
    namespaces = catalog.listNamespaces(Namespace.of("b"));
    assertEquals(2, namespaces.size());
  }
}
