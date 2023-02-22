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

import java.util.Arrays;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestInformationSchema extends SparkTestBaseWithCatalog {
  @Before
  public void createNamespaces() {
    validationNamespaceCatalog.createNamespace(Namespace.of("a.b"));
    validationNamespaceCatalog.createNamespace(Namespace.of("a", "c"));
    validationNamespaceCatalog.createNamespace(Namespace.of("b", "c"));
    validationNamespaceCatalog.createNamespace(Namespace.of("aa", "bb"));
  }

  @After
  public void cleanNamespaces() {
    validationNamespaceCatalog.dropNamespace(Namespace.of("a.b"));
    validationNamespaceCatalog.dropNamespace(Namespace.of("a", "c"));
    validationNamespaceCatalog.dropNamespace(Namespace.of("b", "c"));
    validationNamespaceCatalog.dropNamespace(Namespace.of("aa", "bb"));
  }

  @Test
  public void testNamespacesTable() {
    assertEquals(
        "Should contain the expected namespaces",
        Arrays.asList(
            row("a", null),
            row("a.b", null),
            row("a.c", "a"),
            row("aa", null),
            row("aa.bb", "aa"),
            row("b", null),
            row("b.c", "b")),
        sql("SELECT * FROM %s.information_schema.namespaces ORDER BY namespace_name", catalogName));
  }

  @Test
  public void testNamespacesTableProjection() {
    Object[] rowOfNull = row((Object) null);
    assertEquals(
        "Should contain the expected namespaces",
        Arrays.asList(rowOfNull, rowOfNull, row("a"), rowOfNull, row("aa"), rowOfNull, row("b")),
        sql(
            "SELECT parent_namespace_name FROM %s.information_schema.namespaces ORDER BY namespace_name",
            catalogName));
  }

  @Test
  public void testNamespacesTableFilter() {
    assertEquals(
        "Should contain the expected namespaces",
        Arrays.asList(row("a.c"), row("aa.bb")),
        sql(
            "SELECT namespace_name FROM %s.information_schema.namespaces "
                + "WHERE parent_namespace_name like 'a%%' ORDER BY namespace_name",
            catalogName));
  }
}
